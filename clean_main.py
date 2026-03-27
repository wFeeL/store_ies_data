import json
import math
import statistics
from pathlib import Path

import ips


STATE_FILE = "clean_state.json"
TICKS_LOG_FILE = "clean_ticks_log.txt"

# Биржа
MIN_SELL_PRICE = 5.0
MAX_SELL_PRICE = 20.0
DEFAULT_SELL_PRICE = 8.0
PRICE_UNDERCUT = 0.2
PRICE_RAISE_STEP = 0.1
PRICE_WEAK_FILL_CUT = 0.1
MAX_SELL_ORDERS = 100

# Лестница заявок
USE_PRICE_LADDER = True
LADDER_ORDER_COUNT = 3
LADDER_WEIGHTS = [0.55, 0.30, 0.15]
LADDER_STEP_1 = 0.25
LADDER_STEP_2 = 0.55
MIN_ORDER_ENERGY = 0.15

# Аналитика биржи
MARKET_REF_WINDOW = 5
EXEC_PRICE_WINDOW = 5
FILL_EWMA_ALPHA = 0.35
GOOD_FILL_THRESHOLD = 0.85
WEAK_FILL_THRESHOLD = 0.35
NEAR_ZERO_FILL_THRESHOLD = 0.05
MIN_CONTRACTED_FOR_REAL_FILL = 0.25

# Антидемпинг
ANTI_DUMP_FACTOR = 1.2
ANTI_DUMP_ADDON = 10.0
FIRST_TICK_ANTI_DUMP_LIMIT = 10.0

# Накопители
STORAGE_CAPACITY = 120.0
MAX_CHARGE = 15.0
MAX_DISCHARGE = 20.0
BASE_RESERVE_PER_STORAGE = 20.0

# Прогноз и дефицит
GENERAL_LOOKAHEAD = 8
NIGHT_POST_TAIL = 3
FINAL_WINDOW_MIN = 10

# Ночные режимы
KNOWN_NIGHT_WINDOWS = [(0, 11), (48, 59), (96, 99)]
NIGHT_TARGET_PER_STORAGE = 80.0
STRONG_NIGHT_TARGET_PER_STORAGE = 100.0
LATE_GAME_NIGHT_TARGET_CAP = 60.0

# Погодные пороги
SUN_RELATIVE_DROP_THRESHOLD = 0.85
WIND_RELATIVE_DROP_THRESHOLD = 0.85
SUN_NOW_MIN_FOR_RELATIVE_CHECK = 2.0
WIND_NOW_MIN_FOR_RELATIVE_CHECK = 3.0
ABSOLUTE_BAD_SUN_AVG = 1.5
ABSOLUTE_BAD_WIND_AVG = 3.0
STRONG_NIGHT_WIND_AVG = 4.0
STRONG_NIGHT_WIND_MIN = 2.5
LOAD_SPIKE_FACTOR = 1.10

# Вспомогательные
EPS = 1e-9
FLOOR_EPS = 1e-12
ORDER_ROUND_DIGITS = 3
PRICE_ROUND_DIGITS = 2
LATE_GAME_NIGHT_START_TICK = 96
MAX_BASE_PRICE_DROP_PER_TICK = 0.4

# Быстрая адаптация лестницы на плохом рынке
FAST_MODE_MIN_ASKED = 1.0
BAD_INSTANT_SHARE_THRESHOLD = 0.8
VERY_BAD_FILL_THRESHOLD = 0.01
VERY_BAD_INSTANT_SHARE_THRESHOLD = 0.9
FAST_MODE_STREAK_REQUIRED = 2
FAST_NEAR_ZERO_BULK_DROP = 0.50
FAST_NEAR_ZERO_MID_DROP = 0.25
FAST_NEAR_ZERO_TOP_DROP = 0.10
SOFT_NEAR_ZERO_BULK_DROP = 0.35
SOFT_NEAR_ZERO_MID_DROP = 0.18
SOFT_NEAR_ZERO_TOP_DROP = 0.05
WEAK_FILL_BULK_DROP = 0.20
WEAK_FILL_MID_DROP = 0.10
FAST_SPREAD_MULT = 0.55
SOFT_SPREAD_MULT = 0.75
WEAK_SPREAD_MULT = 0.90


def to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def clamp(value, low, high):
    return max(low, min(high, value))


def weighted_avg(values, weights):
    total_weight = 0.0
    total_value = 0.0
    for value, weight in zip(values, weights):
        w = max(0.0, to_float(weight, 0.0))
        if w <= EPS:
            continue
        total_weight += w
        total_value += to_float(value, 0.0) * w
    if total_weight <= EPS:
        return None
    return total_value / total_weight


def median_or_none(values):
    clean = [to_float(v, 0.0) for v in values]
    if not clean:
        return None
    return float(statistics.median(clean))


def forecast_value(series, idx, fallback=0.0):
    try:
        if idx < 0:
            return float(fallback)
        if idx >= len(series):
            if len(series) == 0:
                return float(fallback)
            return to_float(series[-1], fallback)
        return to_float(series[idx], fallback)
    except Exception:
        return float(fallback)


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


def apply_discharge(storages, amount_limit, reserve_per_storage, allow_below_reserve):
    remaining = max(0.0, amount_limit)
    discharged = 0.0
    for storage in sorted(storages, key=lambda x: x["charge"] + x["planned_charge"] - x["planned_discharge"], reverse=True):
        if remaining <= EPS:
            break
        soc = storage["charge"] + storage["planned_charge"] - storage["planned_discharge"]
        available = max(0.0, soc)
        if not allow_below_reserve:
            available = max(0.0, soc - reserve_per_storage)
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

prev_tick = int(to_float(state.get("prev_tick"), -1))
prev_useful_supplied = max(0.0, to_float(state.get("prev_useful_supplied"), 0.0))
prev_total_external = to_float(state.get("prev_total_external"), 0.0)
prev_generation = max(0.0, to_float(state.get("prev_generation"), 0.0))
prev_consumption = max(0.0, to_float(state.get("prev_consumption"), 0.0))
prev_losses = max(0.0, to_float(state.get("prev_losses"), 0.0))
prev_base_sell_price = clamp(to_float(state.get("prev_base_sell_price"), DEFAULT_SELL_PRICE), MIN_SELL_PRICE, MAX_SELL_PRICE)
prev_solar_factor = max(0.0, to_float(state.get("prev_solar_factor"), 1.0))
prev_wind_factor = max(0.0, to_float(state.get("prev_wind_factor"), 1.0))
prev_fill_ewma = state.get("fill_ewma")
if prev_fill_ewma is not None:
    prev_fill_ewma = clamp(to_float(prev_fill_ewma, 0.0), 0.0, 1.0)

market_history = []
raw_history = state.get("market_history")
if isinstance(raw_history, list):
    for item in raw_history:
        if not isinstance(item, dict):
            continue
        market_history.append(
            {
                "tick": int(to_float(item.get("tick"), 0)),
                "sell_asked": max(0.0, to_float(item.get("sell_asked"), 0.0)),
                "sell_contracted": max(0.0, to_float(item.get("sell_contracted"), 0.0)),
                "sell_instant": max(0.0, to_float(item.get("sell_instant"), 0.0)),
                "sell_fill_rate": None
                if item.get("sell_fill_rate") is None
                else clamp(to_float(item.get("sell_fill_rate"), 0.0), 0.0, 1.0),
                "sell_avg_asked_price": None
                if item.get("sell_avg_asked_price") is None
                else max(0.0, to_float(item.get("sell_avg_asked_price"), 0.0)),
                "sell_avg_contracted_price": None
                if item.get("sell_avg_contracted_price") is None
                else max(0.0, to_float(item.get("sell_avg_contracted_price"), 0.0)),
                "exchange_log_price": None
                if item.get("exchange_log_price") is None
                else max(0.0, to_float(item.get("exchange_log_price"), 0.0)),
            }
        )
if len(market_history) > MARKET_REF_WINDOW:
    market_history = market_history[-MARKET_REF_WINDOW:]


def is_near_zero_history_entry(entry):
    asked = max(0.0, to_float(entry.get("sell_asked"), 0.0))
    if asked < FAST_MODE_MIN_ASKED:
        return False
    fill = entry.get("sell_fill_rate")
    fill_bad = fill is not None and to_float(fill, 1.0) <= NEAR_ZERO_FILL_THRESHOLD
    instant = max(0.0, to_float(entry.get("sell_instant"), 0.0))
    instant_bad = asked > EPS and (instant / asked) >= BAD_INSTANT_SHARE_THRESHOLD
    return fill_bad or instant_bad


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

solar_generation_now = 0.0
wind_generation_now = 0.0

for obj in psm.objects:
    obj_type = str(getattr(obj, "type", "")).strip().lower()
    power_now = getattr(getattr(obj, "power", None), "now", None)
    obj_generated = max(0.0, to_float(getattr(power_now, "generated", 0.0), 0.0))
    obj_consumed = max(0.0, to_float(getattr(power_now, "consumed", 0.0), 0.0))

    current_generation += obj_generated
    current_consumption += obj_consumed

    if obj_type == "housea":
        count_houseA += 1
    elif obj_type == "houseb":
        count_houseB += 1
    elif obj_type == "office":
        count_office += 1
    elif obj_type == "factory":
        count_factory += 1
    elif obj_type == "hospital":
        count_hospital += 1
    elif obj_type == "solar":
        count_solar += 1
        solar_generation_now += obj_generated
    elif obj_type == "wind":
        count_wind += 1
        wind_generation_now += obj_generated
    elif obj_type == "storage":
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

current_external = to_float(getattr(psm.total_power, "external", 0.0), 0.0)
current_losses = max(0.0, to_float(getattr(psm.total_power, "losses", 0.0), 0.0))
current_total_generated = max(0.0, to_float(getattr(psm.total_power, "generated", 0.0), 0.0))
current_total_consumed = max(0.0, to_float(getattr(psm.total_power, "consumed", 0.0), 0.0))

physical_balance_now = current_generation - current_consumption
useful_energy_now = max(0.0, -current_external)

storage_count = count_storage
total_storage_capacity = storage_count * STORAGE_CAPACITY
has_solar = count_solar > 0
has_wind = count_wind > 0

sell_asked = 0.0
sell_contracted = 0.0
sell_instant = 0.0
asked_prices = []
asked_weights = []
contracted_prices = []
contracted_weights = []

for receipt in psm.exchange:
    asked_amount = to_float(getattr(receipt, "askedAmount", 0.0), 0.0)
    if asked_amount >= 0.0:
        continue
    asked_abs = abs(asked_amount)
    contracted_abs = abs(to_float(getattr(receipt, "contractedAmount", 0.0), 0.0))
    instant_abs = abs(to_float(getattr(receipt, "instantAmount", 0.0), 0.0))
    asked_price = abs(to_float(getattr(receipt, "askedPrice", 0.0), 0.0))
    contracted_price = abs(to_float(getattr(receipt, "contractedPrice", 0.0), 0.0))

    sell_asked += asked_abs
    sell_contracted += contracted_abs
    sell_instant += instant_abs

    asked_prices.append(asked_price)
    asked_weights.append(asked_abs)
    if contracted_abs > EPS:
        contracted_prices.append(contracted_price)
        contracted_weights.append(contracted_abs)

sell_fill_rate = None
if sell_asked > EPS:
    sell_fill_rate = sell_contracted / sell_asked

sell_avg_asked_price = weighted_avg(asked_prices, asked_weights)
sell_avg_contracted_price = weighted_avg(contracted_prices, contracted_weights)

good_fill = sell_fill_rate is not None and sell_fill_rate >= GOOD_FILL_THRESHOLD
weak_fill = sell_fill_rate is not None and sell_fill_rate < WEAK_FILL_THRESHOLD
near_zero_fill = (
    sell_asked > EPS
    and (
        (sell_fill_rate is not None and sell_fill_rate <= NEAR_ZERO_FILL_THRESHOLD)
        or sell_contracted < MIN_CONTRACTED_FOR_REAL_FILL
    )
)

sell_instant_share = (sell_instant / sell_asked) if sell_asked > EPS else 0.0
sell_asked_significant = sell_asked >= FAST_MODE_MIN_ASKED
bad_residual_now = sell_asked > EPS and sell_instant_share >= BAD_INSTANT_SHARE_THRESHOLD
near_zero_signal_now = sell_asked_significant and (
    (sell_fill_rate is not None and sell_fill_rate <= NEAR_ZERO_FILL_THRESHOLD)
    or bad_residual_now
)
very_strong_bad_now = sell_asked_significant and (
    (sell_fill_rate is not None and sell_fill_rate <= VERY_BAD_FILL_THRESHOLD)
    or (
        sell_instant_share >= VERY_BAD_INSTANT_SHARE_THRESHOLD
        and sell_contracted < MIN_CONTRACTED_FOR_REAL_FILL
    )
)

if sell_fill_rate is None:
    fill_ewma = prev_fill_ewma
elif prev_fill_ewma is None:
    fill_ewma = sell_fill_rate
else:
    fill_ewma = FILL_EWMA_ALPHA * sell_fill_rate + (1.0 - FILL_EWMA_ALPHA) * prev_fill_ewma
if fill_ewma is not None:
    fill_ewma = clamp(fill_ewma, 0.0, 1.0)

exchange_log_price_last = None
if psm.tick > 0 and len(psm.exchangeLog) >= psm.tick:
    exchange_log_price_last = max(0.0, to_float(psm.exchangeLog[psm.tick - 1], DEFAULT_SELL_PRICE))

market_history.append(
    {
        "tick": max(0, psm.tick - 1),
        "sell_asked": sell_asked,
        "sell_contracted": sell_contracted,
        "sell_instant": sell_instant,
        "sell_fill_rate": sell_fill_rate,
        "sell_avg_asked_price": sell_avg_asked_price,
        "sell_avg_contracted_price": sell_avg_contracted_price,
        "exchange_log_price": exchange_log_price_last,
    }
)
if len(market_history) > MARKET_REF_WINDOW:
    market_history = market_history[-MARKET_REF_WINDOW:]

near_zero_streak = 0
for entry in reversed(market_history):
    if is_near_zero_history_entry(entry):
        near_zero_streak += 1
    else:
        break

fast_bad_market_mode = near_zero_signal_now and (
    near_zero_streak >= FAST_MODE_STREAK_REQUIRED or very_strong_bad_now
)
soft_bad_market_mode = near_zero_signal_now and not fast_bad_market_mode
weak_fill_effective = weak_fill and sell_asked_significant and not near_zero_signal_now

if prev_tick == psm.tick - 1:
    anti_dump_limit = prev_useful_supplied * ANTI_DUMP_FACTOR + ANTI_DUMP_ADDON
else:
    prev_useful_supplied = 0.0
    anti_dump_limit = FIRST_TICK_ANTI_DUMP_LIMIT
anti_dump_limit = max(0.0, anti_dump_limit)


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
        start = t
        while t + 1 < psm.gameLength and abs(forecast_value(psm.forecasts.sun, t + 1, 0.0)) <= EPS:
            t += 1
        night_windows.append((start, t))
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
future_sun_avg = max(0.0, to_float(getattr(psm.sun, "now", 0.0), 0.0))
future_sun_min = future_sun_avg
future_wind_avg = max(0.0, to_float(getattr(psm.wind, "now", 0.0), 0.0))
future_wind_min = future_wind_avg
max_future_load = current_consumption
expected_deficit = 0.0
nearest_deficit_tick = None
nearest_deficit_value = 0.0
nearest_deficit_window_need = 0.0
nearest_deficit_window_open = False
forecast_balance_prefix = 0.0
forecast_balance_prefix_min = 0.0

sun_now = max(0.0, to_float(getattr(psm.sun, "now", 0.0), 0.0))
wind_now = max(0.0, to_float(getattr(psm.wind, "now", 0.0), 0.0))

solar_factor = prev_solar_factor if count_solar > 0 else 0.0
if count_solar > 0 and sun_now > EPS:
    solar_factor = solar_generation_now / max(EPS, count_solar * sun_now)
wind_factor = prev_wind_factor if count_wind > 0 else 0.0
if count_wind > 0 and wind_now > EPS:
    wind_factor = wind_generation_now / max(EPS, count_wind * wind_now)

stable_generation_now = max(0.0, current_generation - solar_generation_now - wind_generation_now)

future_sun_values = []
future_wind_values = []

if lookahead > 0:
    for ti in range(psm.tick + 1, psm.tick + lookahead + 1):
        load_t = max(0.0, future_load_at(ti))
        max_future_load = max(max_future_load, load_t)

        sun_t = max(0.0, forecast_value(psm.forecasts.sun, ti, sun_now))
        wind_t = max(0.0, forecast_value(psm.forecasts.wind, ti, wind_now))
        future_sun_values.append(sun_t)
        future_wind_values.append(wind_t)

        future_solar_generation = count_solar * solar_factor * sun_t
        future_wind_generation = count_wind * wind_factor * wind_t
        generation_t = stable_generation_now + future_solar_generation + future_wind_generation
        balance_t = generation_t - load_t
        deficit_t = max(0.0, -balance_t)
        expected_deficit += deficit_t

        if deficit_t > EPS:
            if nearest_deficit_tick is None:
                nearest_deficit_tick = ti
                nearest_deficit_value = deficit_t
                nearest_deficit_window_need = deficit_t
                nearest_deficit_window_open = True
            elif nearest_deficit_window_open:
                nearest_deficit_window_need += deficit_t
        elif nearest_deficit_window_open:
            nearest_deficit_window_open = False

        forecast_balance_prefix += balance_t
        forecast_balance_prefix_min = min(forecast_balance_prefix_min, forecast_balance_prefix)

    if future_sun_values:
        future_sun_avg = sum(future_sun_values) / len(future_sun_values)
        future_sun_min = min(future_sun_values)
    if future_wind_values:
        future_wind_avg = sum(future_wind_values) / len(future_wind_values)
        future_wind_min = min(future_wind_values)

forecast_buffer_need = max(0.0, -forecast_balance_prefix_min)
nearest_deficit_in_ticks = None if nearest_deficit_tick is None else (nearest_deficit_tick - psm.tick)

future_night_wind_avg = future_wind_avg
future_night_wind_min = future_wind_min
future_night_load_avg = current_consumption
future_night_load_max = current_consumption
if next_night_start is not None and next_night_end is not None:
    night_eval_end = min(next_night_end + NIGHT_POST_TAIL, psm.gameLength - 1)
    night_winds = []
    night_loads = []
    for ti in range(next_night_start, night_eval_end + 1):
        night_winds.append(max(0.0, forecast_value(psm.forecasts.wind, ti, wind_now)))
        night_loads.append(max(0.0, future_load_at(ti)))
    if night_winds:
        future_night_wind_avg = sum(night_winds) / len(night_winds)
        future_night_wind_min = min(night_winds)
    if night_loads:
        future_night_load_avg = sum(night_loads) / len(night_loads)
        future_night_load_max = max(night_loads)

night_target_charge = storage_count * NIGHT_TARGET_PER_STORAGE
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
    and sun_now > SUN_NOW_MIN_FOR_RELATIVE_CHECK
    and future_sun_signal < SUN_RELATIVE_DROP_THRESHOLD * sun_now
)
wind_drop = (
    count_wind > 0
    and wind_now > WIND_NOW_MIN_FOR_RELATIVE_CHECK
    and future_wind_signal < WIND_RELATIVE_DROP_THRESHOLD * wind_now
)
absolute_bad_weather = (
    (count_solar > 0 and future_sun_avg < ABSOLUTE_BAD_SUN_AVG)
    or (count_wind > 0 and future_wind_avg < ABSOLUTE_BAD_WIND_AVG)
)
future_load_risk = (max_future_load > current_generation) or (expected_deficit > EPS)
weather_deficit_risk = future_load_risk and (sun_drop or wind_drop or absolute_bad_weather)
obvious_deficit = nearest_deficit_tick is not None

base_target_charge = storage_count * BASE_RESERVE_PER_STORAGE
strong_night_target_charge = storage_count * STRONG_NIGHT_TARGET_PER_STORAGE
forecast_deficit_target = min(total_storage_capacity, forecast_buffer_need)
nearest_deficit_target = min(total_storage_capacity, nearest_deficit_window_need)

target_charge = max(
    base_target_charge,
    forecast_deficit_target,
    nearest_deficit_target,
    night_target_charge if night_risk else 0.0,
    strong_night_target_charge if night_risk_strong else 0.0,
)
if next_night_start is not None and next_night_start >= LATE_GAME_NIGHT_START_TICK:
    target_charge = min(target_charge, storage_count * LATE_GAME_NIGHT_TARGET_CAP)
target_charge = min(target_charge, total_storage_capacity)

recent_exec = [
    h
    for h in market_history[-EXEC_PRICE_WINDOW:]
    if h.get("sell_avg_contracted_price") is not None and to_float(h.get("sell_contracted"), 0.0) > EPS
]
market_ref_exec = weighted_avg(
    [h["sell_avg_contracted_price"] for h in recent_exec],
    [h["sell_contracted"] for h in recent_exec],
)
executed_volume_window = sum(max(0.0, to_float(h["sell_contracted"], 0.0)) for h in recent_exec)

exchange_log_window = []
if psm.tick > 0:
    log_start = max(0, psm.tick - MARKET_REF_WINDOW)
    for i in range(log_start, psm.tick):
        if i < len(psm.exchangeLog):
            exchange_log_window.append(max(0.0, to_float(psm.exchangeLog[i], DEFAULT_SELL_PRICE)))
if not exchange_log_window:
    exchange_log_window = [h["exchange_log_price"] for h in market_history if h.get("exchange_log_price") is not None]
market_ref_log = median_or_none(exchange_log_window)

if market_ref_exec is not None and executed_volume_window >= MIN_CONTRACTED_FOR_REAL_FILL:
    if market_ref_log is not None:
        market_ref = 0.7 * market_ref_exec + 0.3 * market_ref_log
    else:
        market_ref = market_ref_exec
elif market_ref_log is not None:
    market_ref = market_ref_log
else:
    market_ref = DEFAULT_SELL_PRICE
market_ref = clamp(market_ref, MIN_SELL_PRICE, MAX_SELL_PRICE)

if sell_avg_contracted_price is not None and sell_contracted >= MIN_CONTRACTED_FOR_REAL_FILL:
    raw_base_price = sell_avg_contracted_price - PRICE_UNDERCUT
else:
    raw_base_price = market_ref - PRICE_UNDERCUT

base_sell_price = clamp(raw_base_price, MIN_SELL_PRICE, MAX_SELL_PRICE)
if good_fill and sell_avg_contracted_price is not None and sell_avg_contracted_price >= market_ref:
    base_sell_price = clamp(max(base_sell_price, market_ref + PRICE_RAISE_STEP), MIN_SELL_PRICE, MAX_SELL_PRICE)

if (
    prev_base_sell_price > EPS
    and not good_fill
    and not weak_fill_effective
    and not soft_bad_market_mode
    and not fast_bad_market_mode
):
    base_sell_price = max(base_sell_price, prev_base_sell_price - MAX_BASE_PRICE_DROP_PER_TICK)
base_sell_price = round(clamp(base_sell_price, MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)

final_window = max(FINAL_WINDOW_MIN, psm.gameLength // 10)
in_final_window = psm.tick >= psm.gameLength - final_window

charged_total = 0.0
discharged_total = 0.0
extra_market_discharge = 0.0
effective_useful_energy = useful_energy_now
sell_amount_total = 0.0

risk_ahead = (
    night_risk
    or night_risk_strong
    or weather_deficit_risk
    or obvious_deficit
    or forecast_deficit_target > base_target_charge
)

if physical_balance_now < 0.0:
    deficit = -physical_balance_now
    if in_final_window:
        discharged_total += apply_discharge(storage_objects, deficit, 0.0, True)
    else:
        discharged_primary = apply_discharge(storage_objects, deficit, BASE_RESERVE_PER_STORAGE, False)
        discharged_total += discharged_primary
        remaining = max(0.0, deficit - discharged_primary)
        if remaining > EPS:
            discharged_total += apply_discharge(storage_objects, remaining, 0.0, True)

    post_physical_balance = physical_balance_now + discharged_total
    if in_final_window and storage_count > 0:
        anti_room = max(0.0, anti_dump_limit)
        if anti_room > EPS:
            extra_market_discharge = apply_discharge(storage_objects, anti_room, 0.0, True)
            discharged_total += extra_market_discharge
            effective_useful_energy += extra_market_discharge
            post_physical_balance += extra_market_discharge

    if post_physical_balance > EPS:
        sell_amount_total = min(effective_useful_energy, anti_dump_limit, post_physical_balance)
else:
    if storage_count > 0 and risk_ahead and total_storage_charge < target_charge:
        need_to_target = max(0.0, target_charge - total_storage_charge)
        charge_from_surplus = min(max(0.0, physical_balance_now), need_to_target)
        charged_now = apply_charge(storage_objects, charge_from_surplus)
        charged_total += charged_now
        effective_useful_energy = max(0.0, useful_energy_now - charged_now)

    sell_amount_total = min(effective_useful_energy, anti_dump_limit)

    overflow_energy = max(0.0, effective_useful_energy - sell_amount_total)
    if overflow_energy > EPS and storage_count > 0:
        charged_total += apply_charge(storage_objects, overflow_energy)

    if in_final_window and storage_count > 0:
        anti_room = max(0.0, anti_dump_limit - sell_amount_total)
        if anti_room > EPS:
            extra_market_discharge = apply_discharge(storage_objects, anti_room, 0.0, True)
            if extra_market_discharge > EPS:
                discharged_total += extra_market_discharge
                effective_useful_energy += extra_market_discharge
                sell_amount_total = min(anti_dump_limit, effective_useful_energy)

sell_amount_total = max(0.0, min(sell_amount_total, anti_dump_limit))

ladder_orders = []
if sell_amount_total >= MIN_ORDER_ENERGY:
    if USE_PRICE_LADDER and LADDER_ORDER_COUNT >= 2:
        ladder_count = min(LADDER_ORDER_COUNT, len(LADDER_WEIGHTS), MAX_SELL_ORDERS)
        spread_mult = 1.0
        if fast_bad_market_mode:
            spread_mult = FAST_SPREAD_MULT
        elif soft_bad_market_mode:
            spread_mult = SOFT_SPREAD_MULT
        elif weak_fill_effective:
            spread_mult = WEAK_SPREAD_MULT

        bulk_price = base_sell_price
        mid_price = min(MAX_SELL_PRICE, base_sell_price + LADDER_STEP_1 * spread_mult)
        top_price = min(MAX_SELL_PRICE, base_sell_price + LADDER_STEP_2 * spread_mult)

        if fast_bad_market_mode:
            bulk_price -= FAST_NEAR_ZERO_BULK_DROP
            mid_price -= FAST_NEAR_ZERO_MID_DROP
            top_price -= FAST_NEAR_ZERO_TOP_DROP
        elif soft_bad_market_mode:
            bulk_price -= SOFT_NEAR_ZERO_BULK_DROP
            mid_price -= SOFT_NEAR_ZERO_MID_DROP
            top_price -= SOFT_NEAR_ZERO_TOP_DROP
        elif weak_fill_effective:
            bulk_price -= WEAK_FILL_BULK_DROP
            mid_price -= WEAK_FILL_MID_DROP

        bulk_price = round(clamp(bulk_price, MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
        mid_price = round(clamp(max(mid_price, bulk_price), MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
        top_price = round(clamp(max(top_price, mid_price), MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)

        if fast_bad_market_mode:
            mid_cap = bulk_price + LADDER_STEP_1 * FAST_SPREAD_MULT
            top_cap = bulk_price + LADDER_STEP_2 * FAST_SPREAD_MULT
            mid_price = round(clamp(min(mid_price, mid_cap), MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
            top_price = round(clamp(min(top_price, top_cap), mid_price, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
        elif soft_bad_market_mode:
            mid_cap = bulk_price + LADDER_STEP_1 * SOFT_SPREAD_MULT
            top_cap = bulk_price + LADDER_STEP_2 * SOFT_SPREAD_MULT
            mid_price = round(clamp(min(mid_price, mid_cap), MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
            top_price = round(clamp(min(top_price, top_cap), mid_price, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)

        prices = [bulk_price]
        if ladder_count >= 2:
            prices.append(mid_price)
        if ladder_count >= 3:
            prices.append(top_price)

        weights = LADDER_WEIGHTS[:ladder_count]
        weight_sum = sum(max(0.0, to_float(w, 0.0)) for w in weights)
        if weight_sum <= EPS:
            weights = [1.0] + [0.0 for _ in range(ladder_count - 1)]
            weight_sum = 1.0
        raw_amounts = [sell_amount_total * (max(0.0, to_float(w, 0.0)) / weight_sum) for w in weights]
        rounded_amounts = [order_amount(a) for a in raw_amounts]
        remainder = order_amount(sell_amount_total - sum(rounded_amounts))
        if remainder > EPS and rounded_amounts:
            rounded_amounts[0] = order_amount(rounded_amounts[0] + remainder)

        spill = 0.0
        for amount, price in zip(rounded_amounts, prices):
            if amount >= MIN_ORDER_ENERGY:
                ladder_orders.append([amount, price])
            else:
                spill += amount
        if ladder_orders and spill > EPS:
            ladder_orders[0][0] = order_amount(ladder_orders[0][0] + spill)
        if not ladder_orders and sell_amount_total >= MIN_ORDER_ENERGY:
            ladder_orders = [[order_amount(sell_amount_total), prices[0]]]
    else:
        ladder_orders = [[order_amount(sell_amount_total), base_sell_price]]

ladder_orders = [
    [amount, round(clamp(price, MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)]
    for amount, price in ladder_orders
    if amount > EPS
]
if len(ladder_orders) > MAX_SELL_ORDERS:
    ladder_orders = ladder_orders[:MAX_SELL_ORDERS]

if ladder_orders:
    total_ladder_amount = sum(amount for amount, _ in ladder_orders)
    if total_ladder_amount > sell_amount_total + EPS:
        cut = total_ladder_amount - sell_amount_total
        ladder_orders[0][0] = order_amount(max(0.0, ladder_orders[0][0] - cut))
        ladder_orders = [x for x in ladder_orders if x[0] >= MIN_ORDER_ENERGY]

ordered_discharged_total = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_discharge"])
    if amount > EPS:
        psm.orders.discharge(storage["id"], amount)
        ordered_discharged_total += amount

ordered_charged_total = 0.0
for storage in storage_objects:
    amount = order_amount(storage["planned_charge"])
    if amount > EPS:
        psm.orders.charge(storage["id"], amount)
        ordered_charged_total += amount

final_ladder = []
for amount, price in ladder_orders:
    a = order_amount(amount)
    p = round(clamp(price, MIN_SELL_PRICE, MAX_SELL_PRICE), PRICE_ROUND_DIGITS)
    if a >= MIN_ORDER_ENERGY:
        psm.orders.sell(a, p)
        final_ladder.append((a, p))

ladder_str = "NONE"
if final_ladder:
    ladder_str = "|".join(f"{a:.3f}@{p:.2f}" for a, p in final_ladder)

nearest_deficit_tick_str = "NA" if nearest_deficit_tick is None else str(int(nearest_deficit_tick))
nearest_deficit_in_str = "NA" if nearest_deficit_in_ticks is None else str(int(nearest_deficit_in_ticks))

tick_log_line = (
    f"TICK={psm.tick} "
    f"GEN={current_generation:.3f} "
    f"CONS={current_consumption:.3f} "
    f"EXTERNAL={current_external:.3f} "
    f"LOSSES={current_losses:.3f} "
    f"PHYSICAL_BALANCE={physical_balance_now:.3f} "
    f"USEFUL_ENERGY_NOW={useful_energy_now:.3f} "
    f"PREV_USEFUL_SUPPLIED={prev_useful_supplied:.3f} "
    f"ANTI_DUMP_LIMIT={anti_dump_limit:.3f} "
    f"SELL_ASKED_LAST={sell_asked:.3f} "
    f"SELL_CONTRACTED_LAST={sell_contracted:.3f} "
    f"SELL_FILL_RATE_LAST={'NA' if sell_fill_rate is None else f'{sell_fill_rate:.3f}'} "
    f"SELL_AVG_EXEC_PRICE_LAST={'NA' if sell_avg_contracted_price is None else f'{sell_avg_contracted_price:.3f}'} "
    f"MARKET_REF={market_ref:.3f} "
    f"BASE_SELL_PRICE={base_sell_price:.2f} "
    f"LADDER={ladder_str} "
    f"TOTAL_SOC={total_storage_charge:.3f} "
    f"TARGET_CHARGE={target_charge:.3f} "
    f"EXPECTED_DEFICIT_SUM={expected_deficit:.3f} "
    f"FORECAST_BUFFER_NEED={forecast_buffer_need:.3f} "
    f"NEAREST_DEFICIT_TICK={nearest_deficit_tick_str} "
    f"NEAREST_DEFICIT_IN={nearest_deficit_in_str} "
    f"NEAREST_DEFICIT_PWR={nearest_deficit_value:.3f} "
    f"NEAREST_DEFICIT_WINDOW={nearest_deficit_window_need:.3f} "
    f"NIGHT_RISK={night_risk} "
    f"WEATHER_DEFICIT_RISK={weather_deficit_risk} "
    f"OBVIOUS_DEFICIT={obvious_deficit} "
    f"CHARGED_TOTAL={ordered_charged_total:.3f} "
    f"DISCHARGED_TOTAL={ordered_discharged_total:.3f}"
)
print(tick_log_line)

try:
    log_path = Path(TICKS_LOG_FILE)
    log_mode = "w" if psm.tick == 0 else "a"
    with log_path.open(log_mode, encoding="utf-8") as file:
        file.write(tick_log_line + "\n")
except Exception:
    pass

new_state = {
    "prev_tick": int(psm.tick),
    "prev_useful_supplied": round(useful_energy_now, 6),
    "prev_total_external": round(current_external, 6),
    "prev_generation": round(current_total_generated, 6),
    "prev_consumption": round(current_total_consumed, 6),
    "prev_losses": round(current_losses, 6),
    "prev_base_sell_price": round(base_sell_price, PRICE_ROUND_DIGITS),
    "prev_solar_factor": round(max(0.0, solar_factor), 6),
    "prev_wind_factor": round(max(0.0, wind_factor), 6),
    "fill_ewma": None if fill_ewma is None else round(fill_ewma, 6),
    "market_history": [
        {
            "tick": int(item["tick"]),
            "sell_asked": round(to_float(item["sell_asked"], 0.0), 6),
            "sell_contracted": round(to_float(item["sell_contracted"], 0.0), 6),
            "sell_instant": round(to_float(item.get("sell_instant"), 0.0), 6),
            "sell_fill_rate": None
            if item.get("sell_fill_rate") is None
            else round(clamp(to_float(item["sell_fill_rate"], 0.0), 0.0, 1.0), 6),
            "sell_avg_asked_price": None
            if item.get("sell_avg_asked_price") is None
            else round(max(0.0, to_float(item["sell_avg_asked_price"], 0.0)), 6),
            "sell_avg_contracted_price": None
            if item.get("sell_avg_contracted_price") is None
            else round(max(0.0, to_float(item["sell_avg_contracted_price"], 0.0)), 6),
            "exchange_log_price": None
            if item.get("exchange_log_price") is None
            else round(max(0.0, to_float(item["exchange_log_price"], 0.0)), 6),
        }
        for item in market_history[-MARKET_REF_WINDOW:]
    ],
}
try:
    state_path.write_text(json.dumps(new_state, ensure_ascii=False), encoding="utf-8")
except Exception:
    pass

psm.save_and_exit()
