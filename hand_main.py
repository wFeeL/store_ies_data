import math
import json
from pathlib import Path

import ips


# --- Настройки (можно менять вручную) ---
MANUAL_SELL_PRICE = 8.0

# Параметры накопителей
STORAGE_CAPACITY = 120.0
MAX_CHARGE = 15.0
MAX_DISCHARGE = 20.0

# Точность заявок
ORDER_ROUND_DIGITS = 3
PRICE_ROUND_DIGITS = 2
EPS = 1e-9
FLOOR_EPS = 1e-12
STATE_FILE = "hand_state.json"
RUNTIME_FALLBACK_STATE_FILES = ("clean_state.json",)
DISCHARGE_COOLDOWN_TICKS = 1


def to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def clamp(value, low, high):
    return max(low, min(high, value))


def current_power_snapshot(psm):
    total_power = getattr(psm, "total_power", None)
    generated = max(0.0, to_float(getattr(total_power, "generated", 0.0), 0.0))
    consumed = max(0.0, to_float(getattr(total_power, "consumed", 0.0), 0.0))
    losses = max(0.0, to_float(getattr(total_power, "losses", 0.0), 0.0))
    external = to_float(getattr(total_power, "external", 0.0), 0.0)
    balance_from_external = -external
    # Канонический баланс через API стенда: генерация минус потребление и потери.
    balance_after_consumption = generated - consumed - losses
    surplus_now = max(0.0, balance_after_consumption)
    deficit_now = max(0.0, -balance_after_consumption)
    return {
        "generated": generated,
        "consumed": consumed,
        "losses": losses,
        "external": external,
        "balance_from_external": balance_from_external,
        "balance_after_consumption": balance_after_consumption,
        "surplus_now": surplus_now,
        "deficit_now": deficit_now,
    }


def resolve_runtime_state(state):
    best_tick = int(to_float(state.get("prev_tick"), -1))
    best_useful = max(0.0, to_float(state.get("prev_useful_supplied"), 0.0))
    best_action = str(state.get("prev_storage_action", "idle")).lower()

    for file_name in RUNTIME_FALLBACK_STATE_FILES:
        if file_name == STATE_FILE:
            continue
        try:
            raw = json.loads(Path(file_name).read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        tick = int(to_float(raw.get("prev_tick"), -1))
        if tick > best_tick:
            best_tick = tick
            best_useful = max(0.0, to_float(raw.get("prev_useful_supplied"), 0.0))
            best_action = str(raw.get("prev_storage_action", "idle")).lower()

    return best_tick, best_useful, best_action


def normalize_storage_id(raw_id):
    if isinstance(raw_id, str):
        return raw_id
    if isinstance(raw_id, (tuple, list)) and len(raw_id) >= 2:
        head = str(raw_id[0]).lower()
        if "storage" in head:
            return f"c{raw_id[1]}"
    return str(raw_id)


def storage_order_id(obj):
    address = getattr(obj, "address", None)
    if isinstance(address, (list, tuple)) and address:
        addr0 = str(address[0])
        if addr0:
            return addr0
    return normalize_storage_id(getattr(obj, "id", ""))


def order_amount(value):
    scale = 10 ** ORDER_ROUND_DIGITS
    clipped = max(0.0, to_float(value, 0.0))
    return math.floor(clipped * scale + FLOOR_EPS) / scale


def apply_charge(storages, amount_limit):
    remaining = max(0.0, amount_limit)
    charged = 0.0
    for storage in sorted(storages, key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"]):
        if remaining <= EPS:
            break
        soc = storage["charge"] + storage["planned_charge"] - storage["planned_discharge"]
        room_capacity = max(0.0, STORAGE_CAPACITY - soc)
        room_rate = max(0.0, MAX_CHARGE - storage["planned_charge"])
        amount = min(remaining, room_capacity, room_rate)
        if amount > EPS:
            storage["planned_charge"] += amount
            charged += amount
            remaining -= amount
    return charged


def apply_discharge(storages, amount_limit):
    remaining = max(0.0, amount_limit)
    discharged = 0.0
    for storage in sorted(storages, key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"], reverse=True):
        if remaining <= EPS:
            break
        soc = storage["charge"] + storage["planned_charge"] - storage["planned_discharge"]
        available = max(0.0, soc)
        room_rate = max(0.0, MAX_DISCHARGE - storage["planned_discharge"])
        amount = min(remaining, available, room_rate)
        if amount > EPS:
            storage["planned_discharge"] += amount
            discharged += amount
            remaining -= amount
    return discharged


psm = ips.init()
state_path = Path(STATE_FILE)
state = {}
try:
    raw_state = json.loads(state_path.read_text(encoding="utf-8"))
    if isinstance(raw_state, dict):
        state = raw_state
except Exception:
    state = {}

storage_objects = []

for obj in psm.objects:
    obj_type = str(getattr(obj, "type", "")).strip().lower()
    if obj_type == "storage":
        if to_float(getattr(obj, "failed", 0), 0.0) > 0:
            continue
        charge_now = max(0.0, to_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0))
        storage_objects.append(
            {
                "id": storage_order_id(obj),
                "charge": charge_now,
                "planned_charge": 0.0,
                "planned_discharge": 0.0,
            }
        )

storage_count = len(storage_objects)
power_snapshot = current_power_snapshot(psm)
current_generated = power_snapshot["generated"]
current_consumed = power_snapshot["consumed"]
current_losses = power_snapshot["losses"]
current_external = power_snapshot["external"]
balance_from_external = power_snapshot["balance_from_external"]
energy_after_consumption_now = power_snapshot["balance_after_consumption"]
surplus_now = power_snapshot["surplus_now"]
deficit_now = power_snapshot["deficit_now"]

cfg = getattr(psm, "config", {})
market_min_price = to_float(getattr(cfg, "get", lambda *_: 2.0)("exchangeExternalSell", 2.0), 2.0)
market_max_price = to_float(getattr(cfg, "get", lambda *_: 20.0)("exchangeExternalBuy", 20.0), 20.0)
if market_min_price > market_max_price:
    market_min_price, market_max_price = market_max_price, market_min_price

amount_scaler = max(0.0, to_float(getattr(cfg, "get", lambda *_: 1.2)("exchangeAmountScaler", 1.2), 1.2))
amount_buffer = max(0.0, to_float(getattr(cfg, "get", lambda *_: 10.0)("exchangeAmountBuffer", 10.0), 10.0))
prev_tick, prev_useful_supplied, prev_storage_action = resolve_runtime_state(state)
if prev_tick == psm.tick - 1:
    anti_dump_limit = prev_useful_supplied * amount_scaler + amount_buffer
else:
    anti_dump_limit = amount_buffer

sell_amount = 0.0
sell_price = MANUAL_SELL_PRICE
mode = "neutral"

if deficit_now > EPS:
    # Дефицит: закрываем нехватку разрядом накопителей, но без мгновенного отката после заряда.
    mode = "deficit"
    cooldown_active = (
        prev_tick >= 0
        and (psm.tick - prev_tick) <= DISCHARGE_COOLDOWN_TICKS
        and prev_storage_action == "charge"
    )
    if not cooldown_active:
        apply_discharge(storage_objects, deficit_now)
    sell_amount = 0.0
elif surplus_now > EPS:
    # Профицит: сначала продаем, остаток направляем в накопители.
    mode = "surplus"
    sell_amount = min(surplus_now, anti_dump_limit)
    residual = max(0.0, surplus_now - sell_amount)
    if storage_count > 0 and residual > EPS:
        apply_charge(storage_objects, residual)

sell_amount = max(0.0, min(sell_amount, anti_dump_limit))

ordered_discharged = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_discharge"])
    if amount > EPS:
        psm.orders.discharge(storage["id"], amount)
        ordered_discharged += amount

ordered_charged = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_charge"])
    if amount > EPS:
        psm.orders.charge(storage["id"], amount)
        ordered_charged += amount

sell_amount_order = order_amount(sell_amount)
sell_price_order = round(clamp(sell_price, market_min_price, market_max_price), PRICE_ROUND_DIGITS)
if sell_amount_order > EPS:
    psm.orders.sell(sell_amount_order, sell_price_order)

storage_action = "idle"
if ordered_discharged > EPS:
    storage_action = "discharge"
elif ordered_charged > EPS:
    storage_action = "charge"

new_state = {
    "prev_tick": int(psm.tick),
    "prev_useful_supplied": round(max(0.0, energy_after_consumption_now), 6),
    "prev_storage_action": storage_action,
}
try:
    state_path.write_text(json.dumps(new_state, ensure_ascii=False), encoding="utf-8")
except Exception:
    pass

print(
    f"TICK={psm.tick} "
    f"MODE={mode} "
    f"GEN={current_generated:.3f} "
    f"CONS={current_consumed:.3f} "
    f"LOSSES={current_losses:.3f} "
    f"EXTERNAL={current_external:.3f} "
    f"BAL_FROM_EXT={balance_from_external:.3f} "
    f"BAL_AFTER_CONS={energy_after_consumption_now:.3f} "
    f"SURPLUS_NOW={surplus_now:.3f} "
    f"DEFICIT_NOW={deficit_now:.3f} "
    f"ANTI_DUMP_LIMIT={anti_dump_limit:.3f} "
    f"STORAGE_ACTION={storage_action} "
    f"CHARGED={ordered_charged:.3f} "
    f"DISCHARGED={ordered_discharged:.3f} "
    f"SELL={sell_amount_order:.3f}@{sell_price_order:.2f}"
)

psm.save_and_exit()
