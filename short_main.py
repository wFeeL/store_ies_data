import json
import os
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
BALANCE_BUFFER = 0.30
DEFICIT_DISCHARGE_THRESHOLD = 0.12
MAX_SOC_FRAC = 0.92
MIN_FLOOR_FRAC = 0.08
ENDGAME_TICKS = 5
TARGET_MARGIN_FRAC = 0.03
MARKET_BATTERY_RATE_FRAC = 0.35
STATE_FILE = 'short_main_state.json'
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
    buckets: Dict[str, List[Any]] = {
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
        address = getattr(obj, 'address', None)
        storage_id = None
        if isinstance(address, (list, tuple)) and address:
            storage_id = address[0]
        if storage_id is None:
            storage_id = getattr(obj, 'id', None) or 'storage'
        rows.append(
            {
                'id': str(storage_id),
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
        if output_now <= 0.05:
            return 1.0
        return clamp(output_now / max(power_cap, 1.0), 0.25, 1.35)
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
    forecasts = getattr(psm, 'forecasts', None)
    future_weather = {
        'solar': max(0.0, forecast_at(getattr(forecasts, 'sun', []), tick)),
        'wind': max(0.0, forecast_at(getattr(forecasts, 'wind', []), tick)),
    }
    total = 0.0
    for obj in generators:
        kind = obj.type
        signal_key = 'solar' if kind == 'solar' else 'wind'
        output_now = safe_float(getattr(getattr(getattr(obj, 'power', None), 'now', None), 'generated', 0.0), 0.0)
        scale = producer_scale(
            output_now=output_now,
            signal_now=now_weather[signal_key],
            signal_cap=weather_cap[signal_key],
            power_cap=power_cap[signal_key],
        )
        forecast_output = power_cap[signal_key] * future_weather[signal_key] / weather_cap[signal_key]
        if tick == int(getattr(psm, 'tick', 0)) + 1:
            predicted = 0.60 * output_now + 0.40 * forecast_output * scale
        else:
            predicted = forecast_output * scale
        total += max(0.0, predicted)
    return total
def predict_load(psm: Any, consumers: List[Any], tick: int, loss_ratio: float) -> Dict[str, float]:
    forecasts = getattr(psm, 'forecasts', None)
    load = 0.0
    for obj in consumers:
        forecast_name = CONSUMER_TYPES.get(obj.type)
        if not forecast_name:
            continue
        series = getattr(forecasts, forecast_name, [])
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
        0.25,
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
def load_state() -> Dict[str, Any]:
    try:
        with open(os.path.join(os.getcwd(), STATE_FILE), 'r', encoding='utf-8') as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
def save_state(tick: int, useful_energy: float) -> None:
    payload = {
        'tick': int(tick),
        'useful_energy': round(max(0.0, safe_float(useful_energy, 0.0)), 6),
    }
    try:
        with open(os.path.join(os.getcwd(), STATE_FILE), 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False)
    except Exception:
        pass
def choose_storage_targets(total_capacity: float, future: List[Dict[str, float]], tick: int, game_length: int) -> Tuple[float, float]:
    future_balances = [row['balance'] for row in future]
    head_deficit = sum(max(0.0, -value) for value in future_balances[:2])
    tail_deficit = sum(max(0.0, -value) for value in future_balances[2:4])
    all_surplus = sum(max(0.0, value) for value in future_balances[:4])
    floor_soc = total_capacity * 0.08 + 0.14 * head_deficit
    floor_soc = clamp(floor_soc, total_capacity * 0.06, total_capacity * 0.24)
    target_soc = total_capacity * 0.52
    target_soc += 0.65 * head_deficit
    target_soc += 0.25 * tail_deficit
    target_soc -= 0.18 * all_surplus
    target_soc = clamp(target_soc, floor_soc, total_capacity * 0.88)
    if tick >= game_length - 2 * ENDGAME_TICKS:
        target_soc = min(target_soc, total_capacity * 0.40)
    if tick >= game_length - ENDGAME_TICKS:
        floor_soc = min(floor_soc, total_capacity * 0.02)
        target_soc = min(target_soc, total_capacity * 0.10)
    return target_soc, floor_soc
def market_reference(psm: Any, cfg: Dict[str, Any]) -> Dict[str, float]:
    floor = max(2.0, safe_float(cfg.get('exchangeExternalSell', 2.0), 2.0))
    cap = 20.0
    step = max(0.01, safe_float(cfg.get('exchangeConsumerPriceStep', 0.2), 0.2))
    external_buy = max(floor, safe_float(cfg.get('exchangeExternalBuy', 10.0), 10.0))
    instant_buy = max(external_buy, safe_float(cfg.get('exchangeExternalInstantBuy', 12.0), 12.0))
    log_prices: List[float] = []
    contracted_prices: List[float] = []
    asked_prices: List[float] = []
    for value in reversed(list(getattr(psm, 'exchangeLog', []))):
        price = safe_float(value, 0.0)
        if price > 0.0:
            log_prices.append(price)
        if len(log_prices) >= 8:
            break
    for receipt in getattr(psm, 'exchange', []):
        asked_amount = safe_float(getattr(receipt, 'askedAmount', 0.0), 0.0)
        asked = safe_float(getattr(receipt, 'askedPrice', 0.0), 0.0)
        contracted = safe_float(getattr(receipt, 'contractedPrice', 0.0), 0.0)
        if asked_amount < 0.0:
            if contracted > 0.0:
                contracted_prices.append(contracted)
            elif asked > 0.0:
                asked_prices.append(asked)
    if log_prices:
        weights = list(range(len(log_prices), 0, -1))
        reference = sum(price * weight for price, weight in zip(log_prices, weights)) / sum(weights)
    elif contracted_prices:
        reference = avg(contracted_prices, floor + 4.0)
    elif asked_prices:
        reference = avg(asked_prices, floor + 3.0)
    else:
        reference = max(floor + 2.0, min(9.0, external_buy - 1.2))
    reference = clamp(reference, floor + step, cap)
    strong_price = round_price(max(floor + 2.5, external_buy - 2.0), step=step, low=floor, high=cap)
    excellent_price = round_price(max(strong_price + step, instant_buy - 3.0), step=step, low=floor, high=cap)
    return {
        'floor': floor,
        'cap': cap,
        'step': step,
        'reference': reference,
        'external_buy': external_buy,
        'instant_buy': instant_buy,
        'strong_price': strong_price,
        'excellent_price': excellent_price,
    }
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
def distribute_discharge(storages: List[Dict[str, Any]], discharge_total: float, per_cell_limit: float, floor_soc: float, cell_capacity: float) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = discharge_total
    per_cell_floor = clamp(floor_soc / max(1, len(storages)), 0.0, cell_capacity)
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
def decide_storage_actions(
    psm: Any,
    storages: List[Dict[str, Any]],
    balance_now: Dict[str, float],
    future: List[Dict[str, float]],
    price_ctx: Dict[str, float],
) -> Dict[str, Any]:
    if not storages:
        return {
            'mode': 'hold',
            'target_soc': 0.0,
            'floor_soc': 0.0,
            'total_capacity': 0.0,
            'total_soc': 0.0,
            'charge_orders': [],
            'discharge_orders': [],
            'charge_total': 0.0,
            'discharge_total': 0.0,
            'market_discharge': 0.0,
            'hot_market': False,
            'excellent_market': False,
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
    head_deficit = sum(max(0.0, -value) for value in future_balances[:2])
    target_soc, floor_soc = choose_storage_targets(total_capacity, future, tick, game_length)
    hot_market = price_ctx['reference'] >= price_ctx['strong_price']
    excellent_market = price_ctx['reference'] >= price_ctx['excellent_price']
    current_surplus = max(0.0, balance_now['balance'])
    current_deficit = max(0.0, -balance_now['balance'])
    charge_cap = max(0.0, total_capacity * MAX_SOC_FRAC - total_soc)
    discharge_cap = max(0.0, total_soc - floor_soc)
    total_charge_rate = len(storages) * charge_rate
    total_discharge_rate = len(storages) * discharge_rate
    mode = 'hold'
    charge_total = 0.0
    discharge_total = 0.0
    market_discharge = 0.0
    # При дефиците энергия из накопителя идет в сеть до аварийного резерва.
    if current_deficit > DEFICIT_DISCHARGE_THRESHOLD and discharge_cap > 0.0:
        desired = current_deficit + 0.45 * head_deficit
        discharge_total = min(total_discharge_rate, discharge_cap, desired)
        mode = 'discharge'
    elif current_deficit > 0.0 and discharge_cap > 0.0 and total_soc > target_soc + total_capacity * TARGET_MARGIN_FRAC:
        desired = min(current_deficit, 0.35 * total_discharge_rate)
        discharge_total = min(total_discharge_rate, discharge_cap, desired)
        if discharge_total >= MIN_ORDER_VOLUME:
            mode = 'discharge'
    elif current_surplus > BALANCE_BUFFER and charge_cap > 0.0:
        charge_gap = max(0.0, target_soc - total_soc)
        reserveable_surplus = current_surplus
        if hot_market and total_soc >= 0.85 * target_soc:
            reserveable_surplus *= 0.55
        elif excellent_market and total_soc >= 0.75 * target_soc and head_deficit < 1.0:
            reserveable_surplus *= 0.35
        charge_total = min(total_charge_rate, charge_cap, charge_gap, reserveable_surplus)
        if charge_total >= MIN_ORDER_VOLUME:
            mode = 'charge'
    extra_soc = max(0.0, total_soc - max(target_soc + total_capacity * TARGET_MARGIN_FRAC + head_deficit, floor_soc))
    can_sell_from_battery = (
        mode != 'charge'
        and current_surplus > BALANCE_BUFFER
        and extra_soc >= MIN_ORDER_VOLUME
        and head_deficit <= 1.5
        and (hot_market or total_soc >= 0.90 * total_capacity)
        and price_ctx['reference'] >= price_ctx['strong_price']
    )
    if can_sell_from_battery:
        headwind_buffer = 0.25 * head_deficit
        if excellent_market:
            headwind_buffer *= 0.6
        market_budget = max(0.0, extra_soc - headwind_buffer)
        rate_left = max(0.0, total_discharge_rate - discharge_total)
        market_discharge = min(rate_left, MARKET_BATTERY_RATE_FRAC * total_discharge_rate, market_budget)
        if market_discharge >= MIN_ORDER_VOLUME:
            discharge_total += market_discharge
            if mode == 'hold':
                mode = 'discharge'
    charge_orders = distribute_charge(storages, charge_total, charge_rate, cell_capacity)
    discharge_orders = distribute_discharge(storages, discharge_total, discharge_rate, floor_soc, cell_capacity)
    charge_total = round_vol(sum(amount for _, amount in charge_orders))
    discharge_total = round_vol(sum(amount for _, amount in discharge_orders))
    market_discharge = round_vol(min(market_discharge, discharge_total))
    return {
        'mode': mode,
        'target_soc': target_soc,
        'floor_soc': floor_soc,
        'total_capacity': total_capacity,
        'total_soc': total_soc,
        'charge_orders': charge_orders,
        'discharge_orders': discharge_orders,
        'charge_total': charge_total,
        'discharge_total': discharge_total,
        'market_discharge': market_discharge,
        'hot_market': hot_market,
        'excellent_market': excellent_market,
    }
def build_sell_ladder(balance_now: Dict[str, float], storage_plan: Dict[str, Any], future: List[Dict[str, float]], price_ctx: Dict[str, float], anti_dump_limit: float) -> List[Tuple[float, float]]:
    current_surplus = max(0.0, balance_now['balance'])
    if anti_dump_limit <= 0.0:
        return []
    if storage_plan['total_capacity'] > 0.0 and storage_plan['total_soc'] + 0.25 < storage_plan['target_soc']:
        return []
    immediate_sellable = max(0.0, current_surplus - storage_plan['charge_total'])
    sellable = min(immediate_sellable + max(0.0, storage_plan['market_discharge']), max(0.0, anti_dump_limit))
    future_balances = [row['balance'] for row in future]
    head_deficit = sum(max(0.0, -value) for value in future_balances[:2])
    risk_buffer = 0.18 * head_deficit
    if storage_plan['market_discharge'] > 0.0:
        risk_buffer *= 0.6
    if storage_plan['hot_market']:
        risk_buffer *= 0.75
    risk_buffer = max(0.0, risk_buffer)
    sellable = max(0.0, sellable - risk_buffer)
    if sellable < MIN_ORDER_VOLUME:
        return []
    floor = price_ctx['floor']
    cap = price_ctx['cap']
    step = price_ctx['step']
    ref = price_ctx['reference']
    hot_market = storage_plan['hot_market']
    excellent_market = storage_plan['excellent_market']
    if excellent_market:
        prices = [max(floor + step, ref - step), ref, min(cap, ref + 2 * step)]
        shares = [0.55, 0.30, 0.15]
    elif hot_market:
        prices = [max(floor + step, ref - step), min(cap, ref + step), min(cap, ref + 2 * step)]
        shares = [0.60, 0.25, 0.15]
    else:
        prices = [max(floor + step, ref - 2 * step), max(floor + step, ref - step)]
        shares = [0.70, 0.30]
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
        if amount >= MIN_ORDER_VOLUME:
            psm.orders.charge(storage_id, amount)
    for storage_id, amount in storage_plan['discharge_orders']:
        if amount >= MIN_ORDER_VOLUME:
            psm.orders.discharge(storage_id, amount)
def place_market_orders(psm: Any, ladder: List[Tuple[float, float]]) -> None:
    for volume, price in ladder:
        if volume >= MIN_ORDER_VOLUME:
            psm.orders.sell(volume, price)
def controller(psm: Any) -> Dict[str, Any]:
    groups = classify_objects(psm)
    storages = storage_rows(groups['storages'])
    balance_now = current_balance(psm)
    state = load_state()
    cfg = getattr(psm, 'config', {})
    future = forecast_window(psm, groups, cfg, balance_now)
    price_ctx = market_reference(psm, cfg)
    storage_plan = decide_storage_actions(psm, storages, balance_now, future, price_ctx)
    prev_tick = int(safe_float(state.get('tick'), -1))
    prev_useful_energy = safe_float(state.get('useful_energy'), 0.0) if prev_tick == int(getattr(psm, 'tick', 0)) - 1 else 0.0
    anti_dump_limit = 1.2 * max(0.0, prev_useful_energy) + 10.0
    ladder = build_sell_ladder(balance_now, storage_plan, future, price_ctx, anti_dump_limit)
    place_storage_orders(psm, storage_plan)
    place_market_orders(psm, ladder)
    save_state(int(getattr(psm, 'tick', 0)), max(0.0, balance_now['balance']))
    return {
        'tick': int(getattr(psm, 'tick', 0)),
        'physical_balance_now': round(balance_now['balance'], 6),
        'market_ref': round(price_ctx['reference'], 6),
        'anti_dump_limit': round(anti_dump_limit, 6),
        'storage_mode': storage_plan['mode'],
        'total_soc': round(storage_plan['total_soc'], 6),
        'target_soc': round(storage_plan['target_soc'], 6),
        'floor_soc': round(storage_plan['floor_soc'], 6),
        'charge_total': round(storage_plan['charge_total'], 6),
        'discharge_total': round(storage_plan['discharge_total'], 6),
        'market_discharge': round(storage_plan['market_discharge'], 6),
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
