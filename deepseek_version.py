import json
import math
from pathlib import Path

import ips

STATE_FILE = "clean_state.json"
TICKS_LOG_FILE = "clean_ticks_log.txt"

# Константы
MIN_SELL_PRICE = 3.0
MAX_SELL_PRICE = 10.0
DEFAULT_SELL_PRICE = 8.0
MAX_SELL_ORDERS = 100
MIN_ORDER_ENERGY = 0.15

STORAGE_CAPACITY = 120.0
MAX_CHARGE = 15.0
MAX_DISCHARGE = 20.0
BASE_RESERVE_PER_STORAGE = 20.0
DISCHARGE_COOLDOWN_TICKS = 1

ANTI_DUMP_FACTOR = 1.2
ANTI_DUMP_ADDON = 10.0
FIRST_TICK_ANTI_DUMP_LIMIT = 10.0

EPS = 1e-9
ORDER_ROUND_DIGITS = 3
PRICE_ROUND_DIGITS = 2


def to_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def order_amount(v):
    scale = 10 ** ORDER_ROUND_DIGITS
    return math.floor(max(0.0, to_float(v, 0.0)) * scale + 1e-12) / scale


def apply_charge(storages, amount):
    remaining = amount
    charged = 0.0
    for s in sorted(storages, key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"]):
        if remaining <= EPS:
            break
        soc = s["charge"] + s["planned_charge"] - s["planned_discharge"]
        room = min(STORAGE_CAPACITY - soc, MAX_CHARGE - s["planned_charge"])
        a = min(remaining, room)
        if a > EPS:
            s["planned_charge"] += a
            charged += a
            remaining -= a
    return charged


def apply_discharge(storages, amount, reserve_per_storage, allow_below):
    remaining = amount
    discharged = 0.0
    for s in sorted(storages, key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"], reverse=True):
        if remaining <= EPS:
            break
        soc = s["charge"] + s["planned_charge"] - s["planned_discharge"]
        avail = soc
        if not allow_below:
            avail = max(0.0, soc - reserve_per_storage)
        room = min(avail, MAX_DISCHARGE - s["planned_discharge"])
        a = min(remaining, room)
        if a > EPS:
            s["planned_discharge"] += a
            discharged += a
            remaining -= a
    return discharged


psm = ips.init()

# Загрузка состояния
state = {}
try:
    with open(STATE_FILE, encoding="utf-8") as f:
        raw = json.load(f)
        if isinstance(raw, dict):
            state = raw
except Exception:
    pass

prev_tick = int(to_float(state.get("prev_tick"), -1))
prev_useful = max(0.0, to_float(state.get("prev_useful_supplied"), 0.0))
prev_action = str(state.get("prev_storage_action", "idle")).lower()

# Собираем накопители
storages = []
for obj in psm.objects:
    if str(getattr(obj, "type", "")).strip().lower() == "storage":
        if to_float(getattr(obj, "failed", 0), 0.0) > 0:
            continue
        charge = max(0.0, to_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0))
        storages.append({
            "id": str(getattr(obj, "address", [""])[0] if getattr(obj, "address", None) else getattr(obj, "id", "")),
            "charge": charge,
            "planned_charge": 0.0,
            "planned_discharge": 0.0,
        })

# Текущий баланс
external = to_float(getattr(psm.total_power, "external", 0.0), 0.0)
surplus_now = max(0.0, -external)
deficit_now = max(0.0, external)

# Цена продажи: последняя цена биржи - 0.4
last_price = DEFAULT_SELL_PRICE
if psm.tick > 0 and len(psm.exchangeLog) >= psm.tick:
    last_price = max(MIN_SELL_PRICE, to_float(psm.exchangeLog[psm.tick - 1], DEFAULT_SELL_PRICE))
sell_price = round(clamp(last_price - 0.4, MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)

# Антидемпинговый лимит продажи
if prev_tick == psm.tick - 1:
    anti_dump_limit = prev_useful * ANTI_DUMP_FACTOR + ANTI_DUMP_ADDON
else:
    anti_dump_limit = FIRST_TICK_ANTI_DUMP_LIMIT
anti_dump_limit = max(0.0, anti_dump_limit)

# Логика
charged_total = 0.0
discharged_total = 0.0
sell_amount = 0.0

if deficit_now > EPS:
    # Дефицит – разряжаем накопители
    cooldown = (prev_tick >= 0 and (psm.tick - prev_tick) <= DISCHARGE_COOLDOWN_TICKS and prev_action == "charge")
    if not cooldown:
        # Сначала до резерва
        d1 = apply_discharge(storages, deficit_now, BASE_RESERVE_PER_STORAGE, False)
        discharged_total += d1
        remain = max(0.0, deficit_now - d1)
        if remain > EPS:
            discharged_total += apply_discharge(storages, remain, 0.0, True)
else:
    # Профицит – сначала заряжаем всё, остаток продаём
    available = surplus_now
    charged_total = apply_charge(storages, available)       # заряжаем сколько можем
    remaining = max(0.0, available - charged_total)         # что не влезло
    sell_amount = min(remaining, anti_dump_limit)           # продаём не более лимита

# Отправка приказов
ordered_charged = 0.0
ordered_discharged = 0.0
for s in storages:
    if s["planned_charge"] > EPS:
        am = order_amount(s["planned_charge"])
        if am > EPS:
            psm.orders.charge(s["id"], am)
            ordered_charged += am
    if s["planned_discharge"] > EPS:
        am = order_amount(s["planned_discharge"])
        if am > EPS:
            psm.orders.discharge(s["id"], am)
            ordered_discharged += am

if sell_amount > EPS:
    am = order_amount(sell_amount)
    if am >= MIN_ORDER_ENERGY:
        psm.orders.sell(am, sell_price)

# Определяем действие
action = "discharge" if ordered_discharged > EPS else ("charge" if ordered_charged > EPS else "idle")

# Лог
log_line = (f"TICK={psm.tick} EXT={external:.3f} SURP={surplus_now:.3f} DEF={deficit_now:.3f} "
            f"PRICE={sell_price:.2f} SELL={sell_amount:.3f} CHARGED={ordered_charged:.3f} "
            f"DISCHARGED={ordered_discharged:.3f} ACTION={action}")
print(log_line)
try:
    with open(TICKS_LOG_FILE, "a" if psm.tick else "w", encoding="utf-8") as f:
        f.write(log_line + "\n")
except Exception:
    pass

# Сохраняем состояние
new_state = {
    "prev_tick": psm.tick,
    "prev_useful_supplied": round(surplus_now, 6),
    "prev_storage_action": action,
}
try:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(new_state, f, ensure_ascii=False)
except Exception:
    pass

psm.save_and_exit()