import json
import math
from pathlib import Path

import ips


STATE_FILE = "clean_state.json"
RUNTIME_FALLBACK_STATE_FILES = ("hand_state.json",)

# Биржа и цены
MIN_SELL_PRICE = 5.0
MAX_SELL_PRICE = 20.0
DEFAULT_FIRST_PRICE = 8.0
PRICE_UNDERCUT = 0.2
MAX_SELL_ORDERS = 100
GP_SELL_PRICE = 2.0
GP_INSTANT_BUY_PRICE = 25.0
GP_INSTANT_SELL_PRICE = 1.5

# Антидемпинг
ANTI_DUMP_FACTOR = 1.2
ANTI_DUMP_ADDON = 10.0
FIRST_TICK_ANTI_DUMP_LIMIT = 10.0

# Накопители
STORAGE_CAPACITY = 120.0
MAX_CHARGE = 15.0
MAX_DISCHARGE = 20.0
BASE_RESERVE_PER_STORAGE = 20.0

# Управление окнами прогноза
GENERAL_LOOKAHEAD = 8
NIGHT_POST_TAIL = 3
FINAL_WINDOW_MIN = 10

# Пороги предзаряда
DEFAULT_TARGET_CHARGE_PER_STORAGE = 20.0
NIGHT_TARGET_CHARGE_PER_STORAGE = 80.0
STRONG_NIGHT_TARGET_CHARGE_PER_STORAGE = 100.0
LATE_GAME_NIGHT_TARGET_CAP = 60.0

# Пороговые сигналы генерации и ветра
SUN_RELATIVE_DROP_THRESHOLD = 0.85
WIND_RELATIVE_DROP_THRESHOLD = 0.85
SUN_NOW_MIN_FOR_RELATIVE_CHECK = 2.0
WIND_NOW_MIN_FOR_RELATIVE_CHECK = 3.0
ABSOLUTE_BAD_SUN_AVG = 1.5
ABSOLUTE_BAD_WIND_AVG = 3.0
STRONG_NIGHT_WIND_AVG = 4.0
STRONG_NIGHT_WIND_MIN = 2.5
LOAD_SPIKE_FACTOR = 1.10

# Ночные окна в последних играх
KNOWN_NIGHT_WINDOWS = [(0, 11), (48, 59), (96, 99)]

# Вспомогательные константы
EPS = 1e-9
FLOOR_EPS = 1e-12
ORDER_ROUND_DIGITS = 3
PRICE_ROUND_DIGITS = 2
LATE_GAME_NIGHT_START_TICK = 96
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


def resolve_runtime_state(state):
    best_tick = int(to_float(state.get("prev_tick"), -1))
    best_useful = max(0.0, to_float(state.get("prev_safe_energy"), 0.0))
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
        kind = str(raw_id[0]).lower()
        if "storage" in kind:
            return f"c{raw_id[1]}"
    return str(raw_id)


def storage_order_id(obj):
    address = getattr(obj, "address", None)
    if isinstance(address, (list, tuple)) and address:
        addr0 = str(address[0])
        if addr0:
            return addr0
    return normalize_storage_id(getattr(obj, "id", ""))


def forecast_value(series, idx, fallback=0.0):
    try:
        if idx < 0:
            return float(fallback)
        if idx >= len(series):
            return to_float(series[-1], fallback) if len(series) else float(fallback)
        return to_float(series[idx], fallback)
    except Exception:
        return float(fallback)


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
        room_by_capacity = max(0.0, STORAGE_CAPACITY - soc)
        room_by_rate = max(0.0, MAX_CHARGE - storage["planned_charge"])
        amount = min(remaining, room_by_capacity, room_by_rate)
        if amount > EPS:
            storage["planned_charge"] += amount
            charged += amount
            remaining -= amount
    return charged


def apply_discharge(storages, amount_limit, reserve, allow_below_reserve):
    remaining = max(0.0, amount_limit)
    discharged = 0.0
    for storage in sorted(
        storages,
        key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"],
        reverse=True,
    ):
        if remaining <= EPS:
            break
        soc = storage["charge"] + storage["planned_charge"] - storage["planned_discharge"]
        available = max(0.0, soc) if allow_below_reserve else max(0.0, soc - reserve)
        room_by_rate = max(0.0, MAX_DISCHARGE - storage["planned_discharge"])
        amount = min(remaining, available, room_by_rate)
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

prev_tick = int(to_float(state.get("prev_tick"), -1))
prev_generation = max(0.0, to_float(state.get("prev_generation"), 0.0))
prev_consumption = max(0.0, to_float(state.get("prev_consumption"), 0.0))
prev_safe_energy = max(0.0, to_float(state.get("prev_safe_energy"), 0.0))
prev_storage_action = str(state.get("prev_storage_action", "idle")).lower()
runtime_tick, runtime_useful, runtime_action = resolve_runtime_state(state)
if runtime_tick > prev_tick:
    prev_tick = runtime_tick
    prev_safe_energy = runtime_useful
    prev_storage_action = runtime_action

if prev_tick == psm.tick - 1:
    anti_dump_limit = prev_safe_energy * ANTI_DUMP_FACTOR + ANTI_DUMP_ADDON
else:
    prev_safe_energy = 0.0
    anti_dump_limit = FIRST_TICK_ANTI_DUMP_LIMIT

anti_dump_limit = max(0.0, anti_dump_limit)

current_generation = 0.0
current_consumption = 0.0
total_storage_charge = 0.0
storage_objects = []

count_houseA = 0
count_houseB = 0
count_office = 0
count_factory = 0
count_hospital = 0
count_solar = 0
count_wind = 0
count_storage = 0

for obj in psm.objects:
    obj_type = str(getattr(obj, "type", ""))
    obj_type_norm = obj_type.strip().lower()

    power_now = getattr(getattr(obj, "power", None), "now", None)
    current_generation += max(0.0, to_float(getattr(power_now, "generated", 0.0), 0.0))
    current_consumption += max(0.0, to_float(getattr(power_now, "consumed", 0.0), 0.0))

    if obj_type_norm == "housea":
        count_houseA += 1
    elif obj_type_norm == "houseb":
        count_houseB += 1
    elif obj_type_norm == "office":
        count_office += 1
    elif obj_type_norm == "factory":
        count_factory += 1
    elif obj_type_norm == "hospital":
        count_hospital += 1
    elif obj_type_norm == "solar":
        count_solar += 1
    elif obj_type_norm == "wind":
        count_wind += 1
    elif obj_type_norm == "storage":
        if to_float(getattr(obj, "failed", 0), 0.0) > 0:
            continue
        count_storage += 1
        charge_now = max(0.0, to_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0))
        storage_objects.append(
            {
                "id": storage_order_id(obj),
                "charge": charge_now,
                "planned_charge": 0.0,
                "planned_discharge": 0.0,
            }
        )
        total_storage_charge += charge_now

solar_count = count_solar
wind_count = count_wind
has_solar = count_solar > 0
has_wind = count_wind > 0
storage_count = count_storage
total_storage_capacity = storage_count * STORAGE_CAPACITY

safe_energy_raw = current_generation - current_consumption
safe_energy = max(0.0, safe_energy_raw)
current_external = to_float(getattr(psm.total_power, "external", 0.0), 0.0)
useful_energy_now = max(0.0, -current_external)


def future_load_at(tick_index):
    return (
        count_houseA * forecast_value(psm.forecasts.houseA, tick_index, 0.0)
        + count_houseB * forecast_value(psm.forecasts.houseB, tick_index, 0.0)
        + count_office * forecast_value(psm.forecasts.office, tick_index, 0.0)
        + count_factory * forecast_value(psm.forecasts.factory, tick_index, 0.0)
        + count_hospital * forecast_value(psm.forecasts.hospital, tick_index, 0.0)
    )


night_windows = []
t = 0
while t < psm.gameLength:
    if abs(forecast_value(psm.forecasts.sun, t, 0.0)) <= EPS:
        night_start = t
        while t + 1 < psm.gameLength and abs(forecast_value(psm.forecasts.sun, t + 1, 0.0)) <= EPS:
            t += 1
        night_windows.append((night_start, t))
    t += 1

if not night_windows:
    for start, end in KNOWN_NIGHT_WINDOWS:
        if start < psm.gameLength:
            night_windows.append((start, min(end, psm.gameLength - 1)))

night_windows.sort()

next_night_start = None
next_night_end = None
for start, end in night_windows:
    if end < psm.tick:
        continue
    next_night_start = start
    next_night_end = end
    break

lookahead = min(GENERAL_LOOKAHEAD, psm.gameLength - psm.tick - 1)

future_sun_avg = max(0.0, to_float(psm.sun.now, 0.0))
future_sun_min = future_sun_avg
future_wind_avg = max(0.0, to_float(psm.wind.now, 0.0))
future_wind_min = future_wind_avg
max_future_load = current_consumption

if lookahead > 0:
    future_ticks = range(psm.tick + 1, psm.tick + lookahead + 1)
    future_sun_values = [max(0.0, forecast_value(psm.forecasts.sun, ti, psm.sun.now)) for ti in future_ticks]
    future_wind_values = [max(0.0, forecast_value(psm.forecasts.wind, ti, psm.wind.now)) for ti in future_ticks]
    future_load_values = [max(0.0, future_load_at(ti)) for ti in future_ticks]

    if future_sun_values:
        future_sun_avg = sum(future_sun_values) / len(future_sun_values)
        future_sun_min = min(future_sun_values)
    if future_wind_values:
        future_wind_avg = sum(future_wind_values) / len(future_wind_values)
        future_wind_min = min(future_wind_values)
    if future_load_values:
        max_future_load = max(future_load_values)

future_night_wind_avg = future_wind_avg
future_night_wind_min = future_wind_min
future_night_load_avg = current_consumption
future_night_load_max = current_consumption

if next_night_start is not None and next_night_end is not None:
    night_eval_end = min(next_night_end + NIGHT_POST_TAIL, psm.gameLength - 1)
    night_ticks = range(next_night_start, night_eval_end + 1)
    night_wind_values = [max(0.0, forecast_value(psm.forecasts.wind, ti, psm.wind.now)) for ti in night_ticks]
    night_load_values = [max(0.0, future_load_at(ti)) for ti in night_ticks]
    if night_wind_values:
        future_night_wind_avg = sum(night_wind_values) / len(night_wind_values)
        future_night_wind_min = min(night_wind_values)
    if night_load_values:
        future_night_load_avg = sum(night_load_values) / len(night_load_values)
        future_night_load_max = max(night_load_values)

night_target_charge = storage_count * NIGHT_TARGET_CHARGE_PER_STORAGE
total_charge_rate = max(MAX_CHARGE, storage_count * MAX_CHARGE)
ticks_to_fill = math.ceil(max(0.0, night_target_charge - total_storage_charge) / total_charge_rate) if storage_count > 0 else 0
precharge_horizon = min(GENERAL_LOOKAHEAD, ticks_to_fill + 1)

night_risk = (
    has_solar
    and next_night_start is not None
    and 0 <= (next_night_start - psm.tick) <= precharge_horizon
)

night_risk_strong = night_risk and (
    future_night_wind_avg < STRONG_NIGHT_WIND_AVG
    or future_night_wind_min < STRONG_NIGHT_WIND_MIN
    or future_night_load_max > current_consumption * LOAD_SPIKE_FACTOR
)

future_sun_signal = 0.7 * future_sun_avg + 0.3 * future_sun_min
future_wind_signal = 0.6 * future_wind_avg + 0.4 * future_wind_min

sun_drop = (
    count_solar > 0
    and psm.sun.now > SUN_NOW_MIN_FOR_RELATIVE_CHECK
    and future_sun_signal < SUN_RELATIVE_DROP_THRESHOLD * psm.sun.now
)

wind_drop = (
    count_wind > 0
    and psm.wind.now > WIND_NOW_MIN_FOR_RELATIVE_CHECK
    and future_wind_signal < WIND_RELATIVE_DROP_THRESHOLD * psm.wind.now
)

absolute_bad_weather = (
    (count_solar > 0 and future_sun_avg < ABSOLUTE_BAD_SUN_AVG)
    or (count_wind > 0 and future_wind_avg < ABSOLUTE_BAD_WIND_AVG)
)

future_load_risk = max_future_load > current_generation
obvious_deficit = future_load_risk and (sun_drop or wind_drop or absolute_bad_weather)

default_target_charge = storage_count * DEFAULT_TARGET_CHARGE_PER_STORAGE
strong_night_target_charge = storage_count * STRONG_NIGHT_TARGET_CHARGE_PER_STORAGE

target_charge = default_target_charge
if night_risk:
    target_charge = night_target_charge
if night_risk_strong:
    target_charge = strong_night_target_charge
if obvious_deficit:
    target_charge = max(target_charge, night_target_charge)
if next_night_start is not None and next_night_start >= LATE_GAME_NIGHT_START_TICK:
    target_charge = min(target_charge, storage_count * LATE_GAME_NIGHT_TARGET_CAP)
if storage_count > 0:
    target_charge = min(target_charge, total_storage_capacity)
else:
    target_charge = 0.0

last_market_price = None
if psm.tick > 0 and len(psm.exchangeLog) >= psm.tick:
    last_market_price = to_float(psm.exchangeLog[psm.tick - 1], DEFAULT_FIRST_PRICE)
if last_market_price is None:
    bid_price = DEFAULT_FIRST_PRICE
else:
    bid_price = clamp(last_market_price - PRICE_UNDERCUT, MIN_SELL_PRICE, MAX_SELL_PRICE)
bid_price = round(bid_price, PRICE_ROUND_DIGITS)

charged_total = 0.0
discharged_total = 0.0
sell_amount = 0.0

useful_deficit_now = max(0.0, current_external)
if useful_deficit_now > EPS:
    # При дефиците только разряжаем накопители, без встречной продажи/заряда.
    cooldown_active = (
        prev_tick >= 0
        and (psm.tick - prev_tick) <= DISCHARGE_COOLDOWN_TICKS
        and prev_storage_action == "charge"
    )
    if not cooldown_active:
        discharged_primary = apply_discharge(storage_objects, useful_deficit_now, BASE_RESERVE_PER_STORAGE, False)
        discharged_total += discharged_primary
        remaining_deficit = max(0.0, useful_deficit_now - discharged_primary)
        if remaining_deficit > EPS:
            discharged_total += apply_discharge(storage_objects, remaining_deficit, 0.0, True)
else:
    # При профиците сначала продаём, остаток направляем в накопители.
    available_useful = useful_energy_now
    sell_amount = min(available_useful, anti_dump_limit)

    unsold_useful = max(0.0, available_useful - sell_amount)
    if unsold_useful > EPS and storage_count > 0:
        charged_total += apply_charge(storage_objects, unsold_useful)

sell_amount = max(0.0, min(sell_amount, anti_dump_limit))

ordered_discharged_total = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_discharge"])
    if amount > 0.0:
        psm.orders.discharge(storage["id"], amount)
        ordered_discharged_total += amount

ordered_charged_total = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_charge"])
    if amount > 0.0:
        psm.orders.charge(storage["id"], amount)
        ordered_charged_total += amount

sell_amount_order = order_amount(sell_amount)
if sell_amount_order > 0.0:
    psm.orders.sell(sell_amount_order, bid_price)

storage_action = "idle"
if ordered_discharged_total > EPS:
    storage_action = "discharge"
elif ordered_charged_total > EPS:
    storage_action = "charge"

print(
    f"TICK={psm.tick} "
    f"GAME_LENGTH={psm.gameLength} "
    f"GEN={current_generation:.3f} "
    f"CONS={current_consumption:.3f} "
    f"RAW_BALANCE={safe_energy_raw:.3f} "
    f"SAFE_ENERGY={safe_energy:.3f} "
    f"PREV_SAFE={prev_safe_energy:.3f} "
    f"ANTI_DUMP_LIMIT={anti_dump_limit:.3f} "
    f"LAST_MARKET_PRICE={'NA' if last_market_price is None else f'{last_market_price:.3f}'} "
    f"BID_PRICE={bid_price:.2f} "
    f"SELL_AMOUNT={sell_amount_order:.3f} "
    f"TOTAL_STORAGE_CHARGE={total_storage_charge:.3f} "
    f"TARGET_CHARGE={target_charge:.3f} "
    f"NIGHT_RISK={night_risk} "
    f"NIGHT_RISK_STRONG={night_risk_strong} "
    f"OBVIOUS_DEFICIT={obvious_deficit} "
    f"STORAGE_ACTION={storage_action} "
    f"CHARGED_TOTAL={ordered_charged_total:.3f} "
    f"DISCHARGED_TOTAL={ordered_discharged_total:.3f}"
)

new_state = {
    "prev_tick": int(psm.tick),
    "prev_generation": round(current_generation, 6),
    "prev_consumption": round(current_consumption, 6),
    "prev_safe_energy": round(max(0.0, current_generation - current_consumption), 6),
    "prev_storage_action": storage_action,
}
try:
    state_path.write_text(json.dumps(new_state, ensure_ascii=False), encoding="utf-8")
except Exception:
    pass

psm.save_and_exit()
