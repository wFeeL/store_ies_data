import json
import math
import statistics
from pathlib import Path

import ips


STATE_FILE = "clean_state.json"
TICKS_LOG_FILE = "clean_ticks_log.txt"
RUNTIME_FALLBACK_STATE_FILES = ("hand_state.json",)

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

# Вспомогательные
EPS = 1e-9
FLOOR_EPS = 1e-12
ORDER_ROUND_DIGITS = 3
PRICE_ROUND_DIGITS = 2
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

# Простая логика накопителей
DISCHARGE_COOLDOWN_TICKS = 1
ENDGAME_SELL_ONLY_TICKS = 8


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
    generated_from_balance = consumed + losses - external
    balance_from_external = -external
    # Для логики тика используем прямой сигнал API: отрицательный external = профицит, положительный = дефицит.
    balance_after_consumption = balance_from_external
    surplus_now = max(0.0, balance_from_external)
    deficit_now = max(0.0, external)
    return {
        "generated": generated,
        "generated_from_balance": generated_from_balance,
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

prev_tick, prev_useful_supplied, prev_storage_action = resolve_runtime_state(state)
prev_base_sell_price = clamp(to_float(state.get("prev_base_sell_price"), DEFAULT_SELL_PRICE), MIN_SELL_PRICE, MAX_SELL_PRICE)
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
    obj_type = str(getattr(obj, "type", "")).strip().lower()

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
    elif obj_type == "wind":
        count_wind += 1
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

power_snapshot = current_power_snapshot(psm)
current_total_generated = power_snapshot["generated"]
current_generated_from_balance = power_snapshot["generated_from_balance"]
current_total_consumed = power_snapshot["consumed"]
current_losses = power_snapshot["losses"]
current_external = power_snapshot["external"]
balance_from_external = power_snapshot["balance_from_external"]
energy_after_consumption_now = power_snapshot["balance_after_consumption"]
surplus_now = power_snapshot["surplus_now"]
deficit_now = power_snapshot["deficit_now"]

physical_balance_now = current_total_generated - current_total_consumed
useful_energy_now = surplus_now
useful_deficit_now = deficit_now

storage_count = count_storage
game_length = int(to_float(getattr(psm, "gameLength", 0), 0))
endgame_sell_only = game_length > 0 and psm.tick >= max(0, game_length - ENDGAME_SELL_ONLY_TICKS)

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
target_charge = storage_count * BASE_RESERVE_PER_STORAGE

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

charged_total = 0.0
discharged_total = 0.0
sell_amount_total = 0.0
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
    available_useful = useful_energy_now
    if endgame_sell_only:
        # В конце игры не копим новый профицит, а продаем его сразу.
        sell_amount_total = min(available_useful, anti_dump_limit)
    else:
        # При профиците сначала пытаемся накопить всю возможную энергию.
        if storage_count > 0 and available_useful > EPS:
            charged_now = apply_charge(storage_objects, available_useful)
            charged_total += charged_now
            available_useful = max(0.0, available_useful - charged_now)

        # Продаем только то, что не удалось запасти.
        sell_amount_total = min(available_useful, anti_dump_limit)

        unsold_useful = max(0.0, available_useful - sell_amount_total)
        if unsold_useful > EPS and storage_count > 0:
            charged_total += apply_charge(storage_objects, unsold_useful)

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
ordered_sell_total = sum(a for a, _ in final_ladder)

storage_action = "idle"
if ordered_discharged_total > EPS:
    storage_action = "discharge"
elif ordered_charged_total > EPS:
    storage_action = "charge"

tick_log_line = (
    f"TICK={psm.tick} "
    f"GEN={current_total_generated:.3f} "
    f"GEN_FROM_BAL={current_generated_from_balance:.3f} "
    f"CONS={current_total_consumed:.3f} "
    f"EXTERNAL={current_external:.3f} "
    f"LOSSES={current_losses:.3f} "
    f"BAL_FROM_EXT={balance_from_external:.3f} "
    f"PHYSICAL_BALANCE={physical_balance_now:.3f} "
    f"BAL_AFTER_CONS={energy_after_consumption_now:.3f} "
    f"SURPLUS_NOW={surplus_now:.3f} "
    f"DEFICIT_NOW={deficit_now:.3f} "
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
    f"STORAGE_ACTION={storage_action} "
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
    "prev_useful_supplied": round(max(0.0, ordered_sell_total), 6),
    "prev_base_sell_price": round(base_sell_price, PRICE_ROUND_DIGITS),
    "prev_storage_action": storage_action,
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
