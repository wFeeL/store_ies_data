import math
from pathlib import Path

import ips


TICKS_LOG_FILE = "clean_ticks_log.txt"

# Накопители
MAX_DISCHARGE = 20.0

# Вспомогательные
EPS = 1e-9
FLOOR_EPS = 1e-12
ORDER_ROUND_DIGITS = 3


def to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


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
        addr0 = str(address[0]).strip()
        if addr0:
            return addr0
    raw_id = normalize_storage_id(getattr(obj, "id", ""))
    return str(raw_id).strip()


def order_amount(value):
    scale = 10 ** ORDER_ROUND_DIGITS
    clipped = max(0.0, to_float(value, 0.0))
    return math.floor(clipped * scale + FLOOR_EPS) / scale


def get_api_objects(psm):
    objects = getattr(psm, "objects", [])
    if isinstance(objects, (list, tuple)):
        return objects
    return []


def collect_storage_objects(psm):
    storages = []
    for obj in get_api_objects(psm):
        if str(getattr(obj, "type", "")).strip().lower() != "storage":
            continue
        if to_float(getattr(obj, "failed", 0), 0.0) > 0.0:
            continue

        storage_id = storage_order_id(obj)
        if not storage_id:
            continue

        charge_raw = getattr(getattr(obj, "charge", None), "now", None)
        charge_now = max(0.0, to_float(charge_raw, 0.0))

        storages.append(
            {
                "id": storage_id,
                "charge": charge_now,
                "planned_discharge": 0.0,
            }
        )
    return storages


def apply_discharge(storages, amount_limit):
    remaining = max(0.0, amount_limit)
    discharged = 0.0

    # Разряжаем по очереди накопители с наибольшим SOC, пока не закроем дефицит.
    for storage in sorted(storages, key=lambda x: x["charge"] - x["planned_discharge"], reverse=True):
        if remaining <= EPS:
            break
        soc = storage["charge"] - storage["planned_discharge"]
        available = max(0.0, soc)
        room_rate = max(0.0, MAX_DISCHARGE - storage["planned_discharge"])
        amount = min(remaining, available, room_rate)
        if amount > EPS:
            storage["planned_discharge"] += amount
            discharged += amount
            remaining -= amount

    return discharged


psm = ips.init()

storage_objects = collect_storage_objects(psm)

total_power = getattr(psm, "total_power", None)
external = to_float(getattr(total_power, "external", 0.0), 0.0)
useful_deficit_now = max(0.0, external)

if useful_deficit_now > EPS:
    apply_discharge(storage_objects, useful_deficit_now)

ordered_discharged_total = 0.0
total_storage_now = 0.0
storage_levels = []
for storage in storage_objects:
    amount = order_amount(storage["planned_discharge"])
    if amount > EPS:
        psm.orders.discharge(storage["id"], amount)
        ordered_discharged_total += amount
    storage_now = max(0.0, storage["charge"] - amount)
    total_storage_now += storage_now
    storage_levels.append(f"{storage['id']}:{storage_now:.3f}")

storage_levels_str = "NONE"
if storage_levels:
    storage_levels_str = "|".join(storage_levels)

tick = int(to_float(getattr(psm, "tick", 0), 0.0))
tick_log_line = (
    f"TICK={tick} "
    f"DISCHARGED={ordered_discharged_total:.3f} "
    f"TOTAL_STORAGE_NOW={total_storage_now:.3f} "
    f"STORAGE_LEVELS={storage_levels_str} "
)
print(tick_log_line)

try:
    log_path = Path(TICKS_LOG_FILE)
    log_mode = "w" if tick == 0 else "a"
    with log_path.open(log_mode, encoding="utf-8") as file:
        file.write(tick_log_line + "\n")
except Exception:
    pass

psm.save_and_exit()
