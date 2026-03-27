import math
import json
from pathlib import Path

import ips


# --- Настройки (можно менять вручную) ---
DEFICIT_MODE_SELL_PRICE = 7.0
SURPLUS_MODE_SELL_PRICE = 9.0

# В профиците сначала пытаемся накопить энергию до этого уровня на каждый накопитель
SURPLUS_TARGET_PER_STORAGE = 80.0

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


def to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def clamp(value, low, high):
    return max(low, min(high, value))


def normalize_storage_id(raw_id):
    if isinstance(raw_id, str):
        return raw_id
    if isinstance(raw_id, (tuple, list)) and len(raw_id) >= 2:
        head = str(raw_id[0]).lower()
        if "storage" in head:
            return f"c{raw_id[1]}"
    return str(raw_id)


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


def total_soc(storages):
    return sum(max(0.0, s["charge"] + s["planned_charge"] - s["planned_discharge"]) for s in storages)


psm = ips.init()
state_path = Path(STATE_FILE)
state = {}
try:
    raw_state = json.loads(state_path.read_text(encoding="utf-8"))
    if isinstance(raw_state, dict):
        state = raw_state
except Exception:
    state = {}

current_generation = 0.0
current_consumption = 0.0
storage_objects = []

for obj in psm.objects:
    obj_type = str(getattr(obj, "type", "")).lower()
    power_now = getattr(getattr(obj, "power", None), "now", None)
    current_generation += max(0.0, to_float(getattr(power_now, "generated", 0.0), 0.0))
    current_consumption += max(0.0, to_float(getattr(power_now, "consumed", 0.0), 0.0))

    if obj_type == "storage":
        charge_now = max(0.0, to_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0))
        storage_objects.append(
            {
                "id": normalize_storage_id(getattr(obj, "id", "")),
                "charge": charge_now,
                "planned_charge": 0.0,
                "planned_discharge": 0.0,
            }
        )

storage_count = len(storage_objects)
total_power = getattr(psm, "total_power", None)
current_external = to_float(getattr(total_power, "external", 0.0), 0.0)
current_losses = max(0.0, to_float(getattr(total_power, "losses", 0.0), 0.0))
useful_balance = -current_external  # >0: есть полезная энергия к выдаче; <0: дефицит

cfg = getattr(psm, "config", {})
market_min_price = to_float(getattr(cfg, "get", lambda *_: 2.0)("exchangeExternalSell", 2.0), 2.0)
market_max_price = to_float(getattr(cfg, "get", lambda *_: 20.0)("exchangeExternalBuy", 20.0), 20.0)
if market_min_price > market_max_price:
    market_min_price, market_max_price = market_max_price, market_min_price

amount_scaler = max(0.0, to_float(getattr(cfg, "get", lambda *_: 1.2)("exchangeAmountScaler", 1.2), 1.2))
amount_buffer = max(0.0, to_float(getattr(cfg, "get", lambda *_: 10.0)("exchangeAmountBuffer", 10.0), 10.0))
prev_tick = int(to_float(state.get("prev_tick"), -1))
prev_useful_supplied = max(0.0, to_float(state.get("prev_useful_supplied"), 0.0))
if prev_tick == psm.tick - 1:
    anti_dump_limit = prev_useful_supplied * amount_scaler + amount_buffer
else:
    anti_dump_limit = amount_buffer

charged_total = 0.0
discharged_total = 0.0
sell_amount = 0.0
sell_price = SURPLUS_MODE_SELL_PRICE
mode = "surplus"

if useful_balance < 0.0:
    # Дефицит: сначала пытаемся закрыть дефицит разрядом накопителей.
    mode = "deficit"
    deficit = -useful_balance
    discharged_total = apply_discharge(storage_objects, deficit)
    post_balance = useful_balance + discharged_total
    sell_amount = max(0.0, post_balance)
    sell_price = DEFICIT_MODE_SELL_PRICE
else:
    # Профицит: сначала заряжаем накопители, остаток продаем.
    available_energy = max(0.0, useful_balance)
    if storage_count > 0 and available_energy > EPS:
        target_total = min(storage_count * SURPLUS_TARGET_PER_STORAGE, storage_count * STORAGE_CAPACITY)
        to_target = max(0.0, target_total - total_soc(storage_objects))
        charged_total = apply_charge(storage_objects, min(available_energy, to_target))
        available_energy = max(0.0, available_energy - charged_total)

    sell_amount = available_energy
    sell_price = SURPLUS_MODE_SELL_PRICE

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

planned_useful_supplied = max(0.0, useful_balance - ordered_charged + ordered_discharged)
new_state = {
    "prev_tick": int(psm.tick),
    "prev_useful_supplied": round(planned_useful_supplied, 6),
}
try:
    state_path.write_text(json.dumps(new_state, ensure_ascii=False), encoding="utf-8")
except Exception:
    pass

print(
    f"TICK={psm.tick} "
    f"MODE={mode} "
    f"GEN={current_generation:.3f} "
    f"CONS={current_consumption:.3f} "
    f"LOSSES={current_losses:.3f} "
    f"EXTERNAL={current_external:.3f} "
    f"USEFUL_BALANCE={useful_balance:.3f} "
    f"ANTI_DUMP_LIMIT={anti_dump_limit:.3f} "
    f"STORAGE_COUNT={storage_count} "
    f"SOC_TOTAL={total_soc(storage_objects):.3f} "
    f"CHARGED={ordered_charged:.3f} "
    f"DISCHARGED={ordered_discharged:.3f} "
    f"SELL={sell_amount_order:.3f}@{sell_price_order:.2f}"
)

psm.save_and_exit()
