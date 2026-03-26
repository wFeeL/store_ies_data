import json
import os
import traceback
from typing import Any, Dict, List, Tuple

import ips


STATE_FILE = "ies_state.json"
REPORT_FILE = "controller_report.json"
MIN_ORDER_VOLUME = 0.25
MIN_PRICE = 4.0
MAX_PRICE = 20.0

CONSUMER_FORECAST_MAP = {
    "hospital": "hospital",
    "factory": "factory",
    "office": "office",
    "housea": "houseA",
    "houseb": "houseB",
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
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


def mean(values: List[float], default: float = 0.0) -> float:
    if not values:
        return float(default)
    return sum(values) / len(values)


def load_state() -> Dict[str, Any]:
    path = os.path.join(os.getcwd(), STATE_FILE)
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(tick: int, useful_energy: float) -> None:
    path = os.path.join(os.getcwd(), STATE_FILE)
    payload = {
        "tick": int(tick),
        "useful": round(max(0.0, safe_float(useful_energy, 0.0)), 6),
    }
    try:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False)
    except Exception:
        pass


def save_report(report: Dict[str, Any]) -> None:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), REPORT_FILE)
    try:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2)
    except Exception:
        pass


def anti_dump_limit(state: Dict[str, Any], tick: int) -> float:
    prev_tick = int(safe_float(state.get("tick"), -1))
    prev_useful = 0.0
    if prev_tick == tick - 1:
        prev_useful = max(0.0, safe_float(state.get("useful"), 0.0))
    return 1.2 * prev_useful + 10.0


def storage_rows(psm: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for obj in getattr(psm, "objects", []):
        if str(getattr(obj, "type", "")).lower() != "storage":
            continue
        if safe_float(getattr(obj, "failed", 0), 0.0) > 0:
            continue
        address = getattr(obj, "address", None)
        if isinstance(address, (list, tuple)) and address:
            storage_id = address[0]
        else:
            storage_id = str(getattr(obj, "id", "storage"))
        soc = safe_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0)
        rows.append({"id": storage_id, "soc": max(0.0, soc)})
    return rows


def current_balance(psm: Any) -> Dict[str, float]:
    total_power = getattr(psm, "total_power", None)
    generated = safe_float(getattr(total_power, "generated", 0.0), 0.0)
    consumed = safe_float(getattr(total_power, "consumed", 0.0), 0.0)
    losses = safe_float(getattr(total_power, "losses", 0.0), 0.0)
    useful = max(0.0, generated - losses)
    return {
        "generated": generated,
        "consumed": consumed,
        "losses": losses,
        "useful": useful,
        "balance": generated - consumed - losses,
    }


def next_tick_consumption(psm: Any) -> float:
    tick = int(getattr(psm, "tick", 0)) + 1
    forecasts = getattr(psm, "forecasts", None)
    total = 0.0
    for obj in getattr(psm, "objects", []):
        kind = str(getattr(obj, "type", "")).lower()
        forecast_name = CONSUMER_FORECAST_MAP.get(kind)
        if not forecast_name:
            continue
        series = getattr(forecasts, forecast_name, [])
        if not series:
            continue
        idx = max(0, min(tick, len(series) - 1))
        total += safe_float(series[idx], 0.0)
    return total


def market_execution(psm: Any) -> Dict[str, float]:
    asked = 0.0
    contracted = 0.0
    ask_prices: List[float] = []
    contract_prices: List[float] = []

    for receipt in getattr(psm, "exchange", []):
        asked_amount = safe_float(getattr(receipt, "askedAmount", 0.0), 0.0)
        if asked_amount >= 0.0:
            continue
        asked_price = safe_float(getattr(receipt, "askedPrice", 0.0), 0.0)
        contracted_amount = safe_float(getattr(receipt, "contractedAmount", 0.0), 0.0)
        contracted_price = safe_float(getattr(receipt, "contractedPrice", 0.0), 0.0)

        asked += abs(asked_amount)
        contracted += abs(contracted_amount)
        if asked_price > 0.0:
            ask_prices.append(asked_price)
        if abs(contracted_amount) > 0.0 and contracted_price > 0.0:
            contract_prices.append(contracted_price)

    fill = contracted / asked if asked > 1e-9 else 0.75
    return {
        "fill": clamp(fill, 0.0, 1.0),
        "ask_avg": mean(ask_prices, 0.0),
        "contract_avg": mean(contract_prices, 0.0),
    }


def market_prices(psm: Any) -> Dict[str, float]:
    cfg = getattr(psm, "config", {})
    step = max(0.01, safe_float(cfg.get("exchangeConsumerPriceStep", 0.2), 0.2))
    floor = max(MIN_PRICE, safe_float(cfg.get("exchangeExternalSell", MIN_PRICE), MIN_PRICE))
    cap = MAX_PRICE
    ext_buy = safe_float(cfg.get("exchangeExternalBuy", 10.0), 10.0)
    best_sell_ceiling = clamp(ext_buy, floor + step, cap)
    exec_stats = market_execution(psm)

    log_prices: List[float] = []
    for value in list(getattr(psm, "exchangeLog", []))[-10:]:
        price = safe_float(value, 0.0)
        if price > 0.0:
            log_prices.append(price)

    if log_prices:
        reference = sum(log_prices) / len(log_prices)
    elif exec_stats["contract_avg"] > 0.0:
        reference = exec_stats["contract_avg"]
    else:
        reference = ext_buy - step

    if log_prices and exec_stats["contract_avg"] > 0.0:
        reference = 0.6 * reference + 0.4 * exec_stats["contract_avg"]

    base = reference - step
    if exec_stats["fill"] < 0.35:
        base -= 2.0 * step
    elif exec_stats["fill"] < 0.65:
        base -= step
    elif exec_stats["fill"] > 0.90:
        base += 0.5 * step

    if exec_stats["ask_avg"] > 0.0 and exec_stats["fill"] < 0.5:
        base = min(base, exec_stats["ask_avg"] - step)

    base = round_price(base, step=step, low=floor + step, high=best_sell_ceiling)
    low = round_price(base - step, step=step, low=floor + step, high=base)
    high = round_price(base + step, step=step, low=base, high=best_sell_ceiling)

    return {
        "step": step,
        "floor": floor,
        "cap": cap,
        "reference": reference,
        "fill": exec_stats["fill"],
        "low": low,
        "base": base,
        "high": high,
    }


def split_charge(storages: List[Dict[str, Any]], total: float, per_cell_rate: float, cell_capacity: float) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = max(0.0, total)
    for row in sorted(storages, key=lambda item: item["soc"]):
        if remaining < MIN_ORDER_VOLUME:
            break
        room = max(0.0, cell_capacity - row["soc"])
        amount = round_vol(min(remaining, per_cell_rate, room))
        if amount >= MIN_ORDER_VOLUME:
            orders.append((row["id"], amount))
            remaining -= amount
    return orders


def split_discharge(
    storages: List[Dict[str, Any]],
    total: float,
    per_cell_rate: float,
    cell_capacity: float,
    floor_total: float,
) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = max(0.0, total)
    if not storages:
        return orders
    floor_per_cell = clamp(floor_total / len(storages), 0.0, cell_capacity)
    for row in sorted(storages, key=lambda item: item["soc"], reverse=True):
        if remaining < MIN_ORDER_VOLUME:
            break
        available = max(0.0, row["soc"] - floor_per_cell)
        amount = round_vol(min(remaining, per_cell_rate, available))
        if amount >= MIN_ORDER_VOLUME:
            orders.append((row["id"], amount))
            remaining -= amount
    return orders


def build_sell_orders(volume: float, low_price: float, base_price: float, high_price: float, fill: float) -> List[Tuple[float, float]]:
    volume = round_vol(volume)
    if volume < MIN_ORDER_VOLUME:
        return []

    if volume < 3.0:
        shares = [1.0]
        prices = [low_price]
    elif volume < 8.0:
        shares = [0.85, 0.15] if fill < 0.5 else [0.75, 0.25]
        prices = [low_price, base_price]
    else:
        shares = [0.75, 0.20, 0.05] if fill < 0.5 else [0.60, 0.30, 0.10]
        prices = [low_price, base_price, high_price]

    allocations = [round_vol(volume * share) for share in shares]
    allocations[0] = round_vol(allocations[0] + (volume - sum(allocations)))

    merged: Dict[float, float] = {}
    for part, price in zip(allocations, prices):
        if part < MIN_ORDER_VOLUME:
            continue
        merged[price] = round_vol(merged.get(price, 0.0) + part)

    return [(vol, price) for price, vol in sorted(merged.items()) if vol >= MIN_ORDER_VOLUME]


def controller(psm: Any) -> Dict[str, Any]:
    cfg = getattr(psm, "config", {})
    tick = int(getattr(psm, "tick", 0))

    storages = storage_rows(psm)
    cells = len(storages)
    cell_capacity = max(1.0, safe_float(cfg.get("cellCapacity", 120.0), 120.0))
    charge_rate = max(0.0, safe_float(cfg.get("cellChargeRate", 15.0), 15.0))
    discharge_rate = max(0.0, safe_float(cfg.get("cellDischargeRate", 20.0), 20.0))

    total_capacity = cells * cell_capacity
    total_soc = sum(clamp(row["soc"], 0.0, cell_capacity) for row in storages)

    balance_now = current_balance(psm)
    surplus = max(0.0, balance_now["balance"])
    deficit = max(0.0, -balance_now["balance"])

    reserve_floor = 0.15 * total_capacity
    full_threshold = 0.98 * total_capacity

    charge_plan = 0.0
    discharge_for_deficit = 0.0
    discharge_for_market = 0.0

    if deficit >= MIN_ORDER_VOLUME and total_soc > reserve_floor:
        discharge_for_deficit = min(
            deficit,
            cells * discharge_rate,
            max(0.0, total_soc - reserve_floor),
        )
    elif surplus >= MIN_ORDER_VOLUME and cells > 0:
        charge_plan = min(
            surplus,
            cells * charge_rate,
            max(0.0, total_capacity - total_soc),
        )

    if deficit < MIN_ORDER_VOLUME and total_soc >= full_threshold and cells > 0:
        discharge_for_market = min(
            0.35 * cells * discharge_rate,
            max(0.0, total_soc - reserve_floor),
        )

    discharge_plan = discharge_for_deficit + discharge_for_market

    charge_orders = split_charge(storages, charge_plan, charge_rate, cell_capacity)
    discharge_orders = split_discharge(storages, discharge_plan, discharge_rate, cell_capacity, reserve_floor)

    real_charge = sum(amount for _, amount in charge_orders)
    real_discharge = sum(amount for _, amount in discharge_orders)

    market_discharge = 0.0
    if discharge_plan > 0.0 and discharge_for_market > 0.0:
        market_discharge = real_discharge * (discharge_for_market / discharge_plan)

    sell_volume = max(0.0, surplus - real_charge + market_discharge)

    state = load_state()
    sell_volume = min(sell_volume, anti_dump_limit(state, tick))

    prices = market_prices(psm)
    sell_orders = build_sell_orders(
        sell_volume,
        prices["low"],
        prices["base"],
        prices["high"],
        prices["fill"],
    )

    for storage_id, amount in charge_orders:
        psm.orders.charge(storage_id, amount)
    for storage_id, amount in discharge_orders:
        psm.orders.discharge(storage_id, amount)
    for volume, price in sell_orders:
        psm.orders.sell(volume, price)

    save_state(tick, balance_now["useful"])

    mode = "hold"
    if discharge_for_deficit >= MIN_ORDER_VOLUME:
        mode = "discharge_deficit"
    elif charge_plan >= MIN_ORDER_VOLUME:
        mode = "charge_surplus"
    if discharge_for_market >= MIN_ORDER_VOLUME:
        mode = "sell_full_battery" if mode == "hold" else f"{mode}+sell_full_battery"

    return {
        "tick": tick,
        "mode": mode,
        "balance_now": round(balance_now["balance"], 6),
        "next_consumption": round(next_tick_consumption(psm), 6),
        "soc": round(total_soc, 6),
        "capacity": round(total_capacity, 6),
        "charge": round(real_charge, 6),
        "discharge": round(real_discharge, 6),
        "market_ref": round(prices["reference"], 6),
        "market_fill": round(prices["fill"], 6),
        "sell_volume": round(sum(v for v, _ in sell_orders), 6),
        "sell_orders": sell_orders,
    }


def main() -> None:
    psm = ips.init()
    try:
        report = controller(psm)
        save_report(report)
        print(json.dumps(report, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "tick": int(getattr(psm, "tick", 0)),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=False,
            )
        )
    finally:
        if hasattr(psm, "save_and_exit"):
            psm.save_and_exit()


if __name__ == "__main__":
    main()
