import json
import traceback
from typing import Any, Dict, List, Tuple

import ips

CONSUMER_TYPES = {
    'hospital': 'hospital',
    'factory': 'factory',
    'office': 'office',
    'houseA': 'houseA',
    'houseB': 'houseB',
}
GENERATOR_TYPES = {'solar', 'wind'}
STORAGE_TYPE = 'storage'

LOOKAHEAD_TICKS = 4
MIN_ORDER_VOLUME = 0.25
BALANCE_BUFFER = 0.35
MAX_SOC_FRAC = 0.92
MIN_SOC_FRAC = 0.10
ENDGAME_TICKS = 5


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def round_vol(value: float) -> float:
    return round(max(0.0, safe_float(value, 0.0)), 3)


def round_price(value: float, step: float, low: float, high: float) -> float:
    step = max(0.01, safe_float(step, 0.2))
    value = clamp(safe_float(value, low), low, high)
    return round(round(value / step) * step, 2)


def avg(values: List[float], default: float = 0.0) -> float:
    if not values:
        return float(default)
    return sum(values) / float(len(values))


def forecast_at(series: Any, tick: int) -> float:
    if not series:
        return 0.0
    tick = max(0, min(int(tick), len(series) - 1))
    return safe_float(series[tick], 0.0)


def classify_objects(psm: Any) -> Dict[str, List[Any]]:
    buckets = {
        'consumers': [],
        'generators': [],
        'storages': [],
    }
    for obj in getattr(psm, 'objects', []):
        if safe_float(getattr(obj, 'failed', 0), 0.0) > 0:
            continue
        if obj.type in CONSUMER_TYPES:
            buckets['consumers'].append(obj)
        elif obj.type in GENERATOR_TYPES:
            buckets['generators'].append(obj)
        elif obj.type == STORAGE_TYPE:
            buckets['storages'].append(obj)
    return buckets


def storage_rows(storages: List[Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for obj in storages:
        rows.append(
            {
                'id': obj.address[0],
                'soc': safe_float(getattr(getattr(obj, 'charge', None), 'now', 0.0), 0.0),
            }
        )
    return rows


def current_balance(psm: Any) -> Dict[str, float]:
    total_power = getattr(psm, 'total_power', None)
    generated = safe_float(getattr(total_power, 'generated', 0.0), 0.0)
    consumed = safe_float(getattr(total_power, 'consumed', 0.0), 0.0)
    external = safe_float(getattr(total_power, 'external', 0.0), 0.0)
    losses = safe_float(getattr(total_power, 'losses', 0.0), 0.0)
    return {
        'generated': generated,
        'consumed': consumed,
        'external': external,
        'losses': losses,
        'balance': generated - consumed - losses,
    }


def weather_now(psm: Any) -> Dict[str, float]:
    return {
        'solar': max(0.0, safe_float(getattr(getattr(psm, 'sun', None), 'now', 0.0), 0.0)),
        'wind': max(0.0, safe_float(getattr(getattr(psm, 'wind', None), 'now', 0.0), 0.0)),
    }


def producer_scale(output_now: float, signal_now: float, signal_cap: float, power_cap: float) -> float:
    if signal_now <= 0.05 or signal_cap <= 0.0 or power_cap <= 0.0:
        return 1.0 if output_now <= 0.05 else clamp(output_now / max(power_cap, 1.0), 0.25, 1.35)
    theoretical = power_cap * signal_now / signal_cap
    if theoretical <= 0.05:
        return 1.0
    return clamp(output_now / theoretical, 0.25, 1.35)


def predict_generation(psm: Any, generators: List[Any], tick: int, now_weather: Dict[str, float], cfg: Dict[str, Any]) -> float:
    weather_cap = {
        'solar': max(1.0, safe_float(cfg.get('weatherMaxSun', 15.0), 15.0)),
        'wind': max(1.0, safe_float(cfg.get('weatherMaxWind', 15.0), 15.0)),
    }
    power_cap = {
        'solar': max(0.0, safe_float(cfg.get('maxSolarPower', 20.0), 20.0)),
        'wind': max(0.0, safe_float(cfg.get('maxWindPower', 20.0), 20.0)),
    }
    future_weather = {
        'solar': max(0.0, forecast_at(psm.forecasts.sun, tick)),
        'wind': max(0.0, forecast_at(psm.forecasts.wind, tick)),
    }
    total = 0.0
    for obj in generators:
        kind = obj.type
        signal_key = 'solar' if kind == 'solar' else 'wind'
        output_now = safe_float(obj.power.now.generated, 0.0)
        scale = producer_scale(
            output_now=output_now,
            signal_now=now_weather[signal_key],
            signal_cap=weather_cap[signal_key],
            power_cap=power_cap[signal_key],
        )
        forecast_output = power_cap[signal_key] * future_weather[signal_key] / weather_cap[signal_key]
        if tick == getattr(psm, 'tick', 0) + 1:
            predicted = 0.55 * output_now + 0.45 * forecast_output * scale
        else:
            predicted = forecast_output * scale
        total += max(0.0, predicted)
    return total


def predict_load(psm: Any, consumers: List[Any], tick: int, loss_ratio: float) -> Dict[str, float]:
    load = 0.0
    for obj in consumers:
        forecast_name = CONSUMER_TYPES.get(obj.type)
        if not forecast_name:
            continue
        series = getattr(psm.forecasts, forecast_name)
        load += forecast_at(series, tick)
    losses = load * loss_ratio
    return {
        'consumed': load,
        'losses': losses,
        'balance': -load - losses,
    }


def forecast_window(psm: Any, groups: Dict[str, List[Any]], cfg: Dict[str, Any], balance_now: Dict[str, float]) -> List[Dict[str, float]]:
    tick = int(getattr(psm, 'tick', 0))
    now_weather = weather_now(psm)
    loss_ratio = clamp(
        safe_float(balance_now['losses'], 0.0) / max(1.0, safe_float(balance_now['consumed'], 0.0)),
        0.03,
        0.30,
    )
    future: List[Dict[str, float]] = []
    for step in range(1, LOOKAHEAD_TICKS + 1):
        forecast_tick = tick + step
        generation = predict_generation(psm, groups['generators'], forecast_tick, now_weather, cfg)
        load_data = predict_load(psm, groups['consumers'], forecast_tick, loss_ratio)
        future.append(
            {
                'tick': forecast_tick,
                'generated': generation,
                'consumed': load_data['consumed'],
                'losses': load_data['losses'],
                'balance': generation + load_data['balance'],
            }
        )
    return future


def choose_storage_target(total_capacity: float, future_balances: List[float], tick: int, game_length: int) -> Tuple[float, float]:
    future_deficit = sum(max(0.0, -value) for value in future_balances[:3])
    future_surplus = sum(max(0.0, value) for value in future_balances[:3])
    worst_future = min(future_balances) if future_balances else 0.0

    emergency_frac = MIN_SOC_FRAC
    target_frac = 0.55
    if future_deficit > 4.0 or worst_future < -2.0:
        target_frac = 0.72
        emergency_frac = 0.16
    elif future_deficit > future_surplus + 1.0:
        target_frac = 0.64
        emergency_frac = 0.13
    elif future_surplus > future_deficit + 2.0:
        target_frac = 0.40

    if tick >= game_length - ENDGAME_TICKS:
        emergency_frac = 0.03
        target_frac = min(target_frac, 0.22)

    emergency_floor = total_capacity * emergency_frac
    target_soc = clamp(total_capacity * target_frac, emergency_floor, total_capacity * MAX_SOC_FRAC)
    return target_soc, emergency_floor


def distribute_charge(storages: List[Dict[str, Any]], charge_total: float, per_cell_limit: float, cell_capacity: float) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = charge_total
    for storage in sorted(storages, key=lambda row: row['soc']):
        if remaining <= 1e-9:
            break
        room = max(0.0, cell_capacity - storage['soc'])
        amount = min(remaining, per_cell_limit, room)
        amount = round_vol(amount)
        if amount >= MIN_ORDER_VOLUME:
            orders.append((storage['id'], amount))
            remaining -= amount
    return orders


def distribute_discharge(
    storages: List[Dict[str, Any]],
    discharge_total: float,
    per_cell_limit: float,
    protected_floor: float,
    cell_capacity: float,
) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = discharge_total
    per_cell_floor = clamp(protected_floor / max(1, len(storages)), 0.0, cell_capacity)
    for storage in sorted(storages, key=lambda row: row['soc'], reverse=True):
        if remaining <= 1e-9:
            break
        available = max(0.0, storage['soc'] - per_cell_floor)
        amount = min(remaining, per_cell_limit, available)
        amount = round_vol(amount)
        if amount >= MIN_ORDER_VOLUME:
            orders.append((storage['id'], amount))
            remaining -= amount
    return orders


def decide_storage_actions(psm: Any, storages: List[Dict[str, Any]], balance_now: Dict[str, float], future: List[Dict[str, float]]) -> Dict[str, Any]:
    if not storages:
        return {
            'mode': 'hold',
            'target_soc': 0.0,
            'emergency_floor': 0.0,
            'total_capacity': 0.0,
            'total_soc': 0.0,
            'charge_orders': [],
            'discharge_orders': [],
            'charge_total': 0.0,
            'discharge_total': 0.0,
            'market_discharge': 0.0,
        }

    cfg = getattr(psm, 'config', {})
    tick = int(getattr(psm, 'tick', 0))
    game_length = int(getattr(psm, 'gameLength', cfg.get('gameLength', 100)))
    cell_capacity = max(1.0, safe_float(cfg.get('cellCapacity', 120.0), 120.0))
    charge_rate = max(0.0, safe_float(cfg.get('cellChargeRate', 15.0), 15.0))
    discharge_rate = max(0.0, safe_float(cfg.get('cellDischargeRate', 20.0), 20.0))
    total_capacity = len(storages) * cell_capacity
    total_soc = sum(clamp(storage['soc'], 0.0, cell_capacity) for storage in storages)

    future_balances = [row['balance'] for row in future]
    target_soc, emergency_floor = choose_storage_target(total_capacity, future_balances, tick, game_length)
    immediate_surplus = max(0.0, balance_now['balance'])
    immediate_deficit = max(0.0, -balance_now['balance'])
    avg_future = avg(future_balances, balance_now['balance'])
    future_deficit = sum(max(0.0, -value) for value in future_balances[:3])
    future_surplus = sum(max(0.0, value) for value in future_balances[:3])

    charge_cap = max(0.0, total_capacity * MAX_SOC_FRAC - total_soc)
    discharge_cap = max(0.0, total_soc - emergency_floor)
    mode = 'hold'
    charge_total = 0.0
    discharge_total = 0.0
    market_discharge = 0.0

    if immediate_deficit > BALANCE_BUFFER and discharge_cap > 0.0:
        desired = immediate_deficit + 0.25 * max(0.0, -avg_future)
        discharge_total = min(len(storages) * discharge_rate, discharge_cap, desired)
        mode = 'discharge'
    elif immediate_surplus > BALANCE_BUFFER and total_soc < target_soc:
        desired = max(0.0, min(total_capacity * MAX_SOC_FRAC, target_soc) - total_soc)
        if future_deficit > future_surplus:
            desired = max(desired, 0.55 * immediate_surplus)
        charge_total = min(len(storages) * charge_rate, charge_cap, immediate_surplus, desired)
        mode = 'charge'
    elif immediate_surplus > 1.0 and total_soc < target_soc + 0.08 * total_capacity:
        desired = max(0.0, target_soc + 0.08 * total_capacity - total_soc)
        charge_total = min(len(storages) * charge_rate, charge_cap, 0.70 * immediate_surplus, desired)
        mode = 'charge'
    elif total_soc > 0.86 * total_capacity and avg_future > -0.5:
        extra = max(0.0, total_soc - max(target_soc, emergency_floor))
        market_discharge = min(0.5 * len(storages) * discharge_rate, extra)
        if market_discharge >= MIN_ORDER_VOLUME:
            discharge_total = market_discharge
            mode = 'discharge'

    charge_orders = distribute_charge(storages, charge_total, charge_rate, cell_capacity)
    discharge_orders = distribute_discharge(storages, discharge_total, discharge_rate, emergency_floor, cell_capacity)
    charge_total = round_vol(sum(amount for _, amount in charge_orders))
    discharge_total = round_vol(sum(amount for _, amount in discharge_orders))
    market_discharge = round_vol(min(market_discharge, discharge_total))

    return {
        'mode': mode,
        'target_soc': target_soc,
        'emergency_floor': emergency_floor,
        'total_capacity': total_capacity,
        'total_soc': total_soc,
        'charge_orders': charge_orders,
        'discharge_orders': discharge_orders,
        'charge_total': charge_total,
        'discharge_total': discharge_total,
        'market_discharge': market_discharge,
    }


def market_reference(psm: Any, cfg: Dict[str, Any]) -> Dict[str, float]:
    floor = max(0.1, safe_float(cfg.get('exchangeExternalSell', 2.0), 2.0))
    cap = max(floor, safe_float(cfg.get('exchangeExternalBuy', 20.0), 20.0))
    step = max(0.01, safe_float(cfg.get('exchangeConsumerPriceStep', 0.2), 0.2))
    prices: List[float] = []

    for value in reversed(list(getattr(psm, 'exchangeLog', []))):
        price = safe_float(value, 0.0)
        if price > 0.0:
            prices.append(price)
        if len(prices) >= 6:
            break

    for receipt in getattr(psm, 'exchange', []):
        asked = safe_float(getattr(receipt, 'askedPrice', 0.0), 0.0)
        contracted = safe_float(getattr(receipt, 'contractedPrice', 0.0), 0.0)
        if asked > 0.0:
            prices.append(asked)
        if contracted > 0.0:
            prices.append(contracted)

    base_price = avg(prices, floor + 3 * step)
    base_price = clamp(base_price, floor + step, cap)
    return {
        'floor': floor,
        'cap': cap,
        'step': step,
        'reference': base_price,
    }


def build_sell_ladder(
    psm: Any,
    balance_now: Dict[str, float],
    storage_plan: Dict[str, Any],
    future: List[Dict[str, float]],
) -> List[Tuple[float, float]]:
    cfg = getattr(psm, 'config', {})
    price_ctx = market_reference(psm, cfg)
    current_surplus = max(0.0, balance_now['balance'])
    sellable = current_surplus - storage_plan['charge_total']
    sellable += max(0.0, storage_plan['market_discharge'])

    future_balances = [row['balance'] for row in future]
    total_capacity = max(0.0, storage_plan['total_capacity'])
    near_full = total_capacity > 0.0 and storage_plan['total_soc'] > 0.85 * total_capacity
    risk_buffer = max(BALANCE_BUFFER, 0.20 * sum(max(0.0, -value) for value in future_balances[:2]))
    if near_full:
        risk_buffer *= 0.5
    sellable = max(0.0, sellable - risk_buffer)

    if sellable < MIN_ORDER_VOLUME:
        return []

    floor = price_ctx['floor']
    cap = price_ctx['cap']
    step = price_ctx['step']
    ref = price_ctx['reference']
    avg_future = avg(future_balances, balance_now['balance'])

    if near_full or avg_future > 1.0:
        prices = [
            max(floor + step, ref - step),
            min(cap, ref + step),
        ]
        shares = [0.70, 0.30]
    else:
        prices = [
            max(floor + step, ref),
            min(cap, ref + 2 * step),
        ]
        shares = [0.80, 0.20]

    orders: List[Tuple[float, float]] = []
    allocated = 0.0
    for index, share in enumerate(shares):
        if index == len(shares) - 1:
            volume = max(0.0, sellable - allocated)
        else:
            volume = round_vol(sellable * share)
            allocated += volume
        volume = round_vol(volume)
        if volume < MIN_ORDER_VOLUME:
            continue
        price = round_price(prices[index], step=step, low=floor, high=cap)
        orders.append((volume, price))
    return orders


def place_storage_orders(psm: Any, storage_plan: Dict[str, Any]) -> None:
    for storage_id, amount in storage_plan['charge_orders']:
        if amount > 0.0:
            psm.orders.charge(storage_id, amount)
    for storage_id, amount in storage_plan['discharge_orders']:
        if amount > 0.0:
            psm.orders.discharge(storage_id, amount)


def place_market_orders(psm: Any, ladder: List[Tuple[float, float]]) -> None:
    for volume, price in ladder:
        if volume >= MIN_ORDER_VOLUME:
            psm.orders.sell(volume, price)


def controller(psm: Any) -> Dict[str, Any]:
    groups = classify_objects(psm)
    storages = storage_rows(groups['storages'])
    balance_now = current_balance(psm)
    future = forecast_window(psm, groups, getattr(psm, 'config', {}), balance_now)
    storage_plan = decide_storage_actions(psm, storages, balance_now, future)
    ladder = build_sell_ladder(psm, balance_now, storage_plan, future)

    place_storage_orders(psm, storage_plan)
    place_market_orders(psm, ladder)

    return {
        'tick': int(getattr(psm, 'tick', 0)),
        'consumers': len(groups['consumers']),
        'generators': len(groups['generators']),
        'storages': len(groups['storages']),
        'generated_now': round(balance_now['generated'], 6),
        'consumed_now': round(balance_now['consumed'], 6),
        'losses_now': round(balance_now['losses'], 6),
        'external_now': round(balance_now['external'], 6),
        'physical_balance_now': round(balance_now['balance'], 6),
        'future_balances': [round(row['balance'], 6) for row in future],
        'storage_mode': storage_plan['mode'],
        'total_soc': round(storage_plan['total_soc'], 6),
        'target_soc': round(storage_plan['target_soc'], 6),
        'emergency_floor': round(storage_plan['emergency_floor'], 6),
        'charge_orders': storage_plan['charge_orders'],
        'discharge_orders': storage_plan['discharge_orders'],
        'charge_total': round(storage_plan['charge_total'], 6),
        'discharge_total': round(storage_plan['discharge_total'], 6),
        'sell_orders': ladder,
    }


def main() -> None:
    psm = ips.init()
    try:
        print(json.dumps(controller(psm), ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    'tick': int(getattr(psm, 'tick', 0)),
                    'error': str(exc),
                    'traceback': traceback.format_exc(),
                },
                ensure_ascii=False,
            )
        )
    finally:
        if hasattr(psm, 'save_and_exit'):
            psm.save_and_exit()


if __name__ == '__main__':
    main()
