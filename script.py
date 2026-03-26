import ips
import math

psm = ips.init()
print("TICK", psm.tick)

# ----------------- helpers -----------------

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def safe_num(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def avg(values, default=0.0):
    values = [safe_num(v, None) for v in values]
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else default

def round_vol(x):
    return round(max(0.0, x), 3)

def round_price(x):
    return round(clamp(x, 2.0, 20.0), 3)

def obj_type(obj):
    return str(getattr(obj, "type", "")).lower()

def obj_id(obj):
    return getattr(obj, "id", None)

def obj_gen_now(obj):
    try:
        return safe_num(obj.power.now.generated, 0.0)
    except Exception:
        return 0.0

def obj_cons_now(obj):
    try:
        return safe_num(obj.power.now.consumed, 0.0)
    except Exception:
        return 0.0

def obj_soc_now(obj):
    try:
        return safe_num(obj.charge.now, 0.0)
    except Exception:
        return 0.0

def forecast_value(psm, t, idx):
    idx = max(0, idx)

    if t == "housea":
        return safe_num(psm.forecasts.houseA[idx], 0.0)
    if t == "houseb":
        return safe_num(psm.forecasts.houseB[idx], 0.0)
    if t == "factory":
        return safe_num(psm.forecasts.factory[idx], 0.0)
    if t == "office":
        return safe_num(psm.forecasts.office[idx], 0.0)
    if t == "hospital":
        return safe_num(psm.forecasts.hospital[idx], 0.0)

    return 0.0

def receipt_field(r, name, pos, default=0.0):
    try:
        return safe_num(getattr(r, name), default)
    except Exception:
        pass
    try:
        return safe_num(r[pos], default)
    except Exception:
        return default

def market_info(psm):
    exchange = list(getattr(psm, "exchange", []))
    exch_log = list(getattr(psm, "exchangeLog", []))

    # Внешний рыночный референс по последним значениям биржи
    log_tail = [safe_num(x, None) for x in exch_log[-8:]]
    log_tail = [x for x in log_tail if x is not None and x > 0]
    market_ref = avg(log_tail, 5.0)

    sell_asked = 0.0
    sell_contracted = 0.0
    sell_ask_prices = []
    sell_contract_prices = []

    for r in exchange:
        asked_amount = receipt_field(r, "askedAmount", 0, 0.0)
        asked_price = receipt_field(r, "askedPrice", 1, 0.0)
        contracted_amount = receipt_field(r, "contractedAmount", 2, 0.0)
        contracted_price = receipt_field(r, "contractedPrice", 3, 0.0)

        # продажа = askedAmount < 0
        if asked_amount < 0:
            sell_asked += abs(asked_amount)
            sell_contracted += abs(contracted_amount)
            if abs(asked_amount) > 1e-9:
                sell_ask_prices.append(asked_price)
            if abs(contracted_amount) > 1e-9:
                sell_contract_prices.append(contracted_price)

    fill = sell_contracted / sell_asked if sell_asked > 1e-9 else 0.85
    our_ask_ref = avg(sell_ask_prices, market_ref)
    our_contract_ref = avg(sell_contract_prices, market_ref)

    return {
        "market_ref": market_ref,
        "fill": clamp(fill, 0.0, 1.0),
        "our_ask_ref": our_ask_ref,
        "our_contract_ref": our_contract_ref,
        "sell_asked": sell_asked,
        "sell_contracted": sell_contracted,
    }

def split_storage_charge(storages, total_amount, max_rate=15.0, cap=120.0):
    orders = []
    remaining = total_amount

    storages = sorted(storages, key=lambda s: obj_soc_now(s))
    for s in storages:
        if remaining <= 1e-9:
            break
        soc = obj_soc_now(s)
        room = max(0.0, cap - soc)
        amt = min(remaining, max_rate, room)
        if amt > 1e-9:
            orders.append((obj_id(s), round_vol(amt)))
            remaining -= amt
    return orders

def split_storage_discharge(storages, total_amount, floor_soc=20.0, max_rate=20.0):
    orders = []
    remaining = total_amount

    storages = sorted(storages, key=lambda s: -obj_soc_now(s))
    for s in storages:
        if remaining <= 1e-9:
            break
        soc = obj_soc_now(s)
        avail = max(0.0, soc - floor_soc)
        amt = min(remaining, max_rate, avail)
        if amt > 1e-9:
            orders.append((obj_id(s), round_vol(amt)))
            remaining -= amt
    return orders

def build_ladder(sell_volume, market_ref, fill, our_ask_ref):
    if sell_volume < 0.25:
        return []

    # Чем больше объём, тем ближе к рынку bulk-цена
    if sell_volume <= 3.0:
        bulk = market_ref + 0.30
        mid = market_ref + 0.80
        tail = market_ref + 1.20
        shares = [0.50, 0.30, 0.20]
    elif sell_volume <= 8.0:
        bulk = market_ref + 0.00
        mid = market_ref + 0.50
        tail = market_ref + 0.90
        shares = [0.65, 0.25, 0.10]
    elif sell_volume <= 15.0:
        bulk = market_ref - 0.30
        mid = market_ref + 0.20
        tail = market_ref + 0.60
        shares = [0.72, 0.20, 0.08]
    else:
        bulk = market_ref - 0.60
        mid = market_ref - 0.10
        tail = market_ref + 0.30
        shares = [0.78, 0.17, 0.05]

    # Если в прошлый раз стояли выше рынка и нас плохо брали — опускаемся
    overpriced_gap = max(0.0, our_ask_ref - market_ref)
    if fill < 0.25:
        bulk -= 0.80 + 0.50 * overpriced_gap
        mid  -= 0.80
        tail -= 0.60
    elif fill < 0.60:
        bulk -= 0.40 + 0.30 * overpriced_gap
        mid  -= 0.30
        tail -= 0.20
    elif fill > 0.92 and our_ask_ref + 0.10 < market_ref:
        # если рынок был выше нас и всё хорошо разбирали — можно чуть подняться
        bulk += 0.10
        mid  += 0.20
        tail += 0.25

    prices = [
        round_price(bulk),
        round_price(mid),
        round_price(tail),
    ]

    vols = [
        round_vol(sell_volume * shares[0]),
        round_vol(sell_volume * shares[1]),
        round_vol(sell_volume * shares[2]),
    ]

    # добиваем округление в bulk
    diff = round_vol(sell_volume - sum(vols))
    vols[0] = round_vol(vols[0] + diff)

    ladder = []
    for v, p in zip(vols, prices):
        if v >= 0.25:
            ladder.append((v, p))
    return ladder

# ----------------- tick indexes -----------------

past_tick = max(psm.tick - 1, 0)
next_tick = min(psm.tick + 1, len(psm.forecasts.houseA) - 1)

now_wind = safe_num(psm.wind.now, 0.0)
next_wind = safe_num(psm.wind.then[0], now_wind)

now_sun = safe_num(psm.forecasts.sun[psm.tick], 0.0)
next_sun = safe_num(psm.forecasts.sun[next_tick], now_sun)

# ----------------- market -----------------

mkt = market_info(psm)

# ----------------- system forecast -----------------

current_generation = 0.0
current_consumption = 0.0

generation_next = 0.0
consumption_next = 0.0

storages = []

for obj in psm.objects:
    t = obj_type(obj)

    gen_now = obj_gen_now(obj)
    cons_now = obj_cons_now(obj)

    current_generation += gen_now
    current_consumption += cons_now

    if t == "wind":
        # очень простой прогноз ВЭС
        if next_wind >= now_wind:
            generation_next += gen_now * 1.10
        else:
            generation_next += gen_now * 0.85
        continue

    if t == "solar":
        # очень простой прогноз СЭС
        if next_sun >= now_sun:
            generation_next += gen_now * 1.05
        else:
            generation_next += gen_now * 0.85
        continue

    if t == "storage":
        storages.append(obj)
        continue

    consumption_next += forecast_value(psm, t, next_tick)

# ----------------- network losses -----------------

net_losses_now = 0.0
for _, net in psm.networks.items():
    net_losses_now += safe_num(getattr(net, "losses", 0.0), 0.0)

# простой резерв на ошибки прогноза + потери
reserve = 1.0 + 0.70 * net_losses_now + 0.10 * consumption_next

# ----------------- storage policy -----------------

total_soc = sum(obj_soc_now(s) for s in storages)
cells = max(1, len(storages))
capacity_total = 120.0 * cells

# целевой SOC
target_soc = 60.0 * cells
if generation_next - consumption_next < 0:
    target_soc += 15.0 * cells
if mkt["fill"] < 0.60:
    target_soc += 10.0 * cells
target_soc = clamp(target_soc, 35.0 * cells, 90.0 * cells)

balance_now = current_generation - current_consumption
balance_next = generation_next - consumption_next

charge_total = 0.0
discharge_total = 0.0
discharge_for_market = 0.0

# 1. Сначала спасаем систему от дефицита
if balance_next < 0 and total_soc > 20.0 * cells:
    needed = abs(balance_next) + reserve
    discharge_total = min(20.0 * cells, needed, max(0.0, total_soc - 20.0 * cells))

# 2. Если есть профицит и надо добрать SOC — заряжаем
elif balance_next > reserve and total_soc < target_soc:
    charge_total = min(15.0 * cells, balance_next - reserve, max(0.0, target_soc - total_soc))

# 3. Если рынок слабый, лучше перенести часть энергии в накопитель
elif balance_next > 0 and mkt["fill"] < 0.60 and total_soc < target_soc:
    charge_total = min(15.0 * cells, 0.70 * balance_next, max(0.0, target_soc - total_soc))

# 4. Если рынок сильный и SOC высокий — можно чуть разрядить под продажу
elif balance_next > reserve and mkt["fill"] > 0.90 and total_soc > target_soc + 10.0 * cells:
    discharge_for_market = min(
        20.0 * cells,
        max(0.0, total_soc - target_soc),
        0.40 * max(0.0, balance_next - reserve),
    )
    discharge_total = discharge_for_market

charge_orders = split_storage_charge(storages, charge_total)
discharge_orders = split_storage_discharge(storages, discharge_total)

real_charge = sum(v for _, v in charge_orders)
real_discharge = sum(v for _, v in discharge_orders)

# ----------------- sell volume -----------------

projected_sell = generation_next - consumption_next - reserve - real_charge + min(real_discharge, discharge_for_market)
sell_volume = max(0.0, projected_sell)

# рынок слабый -> режем объём
if mkt["fill"] < 0.25:
    sell_volume *= 0.55
elif mkt["fill"] < 0.60:
    sell_volume *= 0.78
elif mkt["fill"] > 0.92:
    sell_volume *= 1.05

sell_volume = round_vol(sell_volume)

# ----------------- price ladder -----------------

ladder = build_ladder(
    sell_volume=sell_volume,
    market_ref=mkt["market_ref"],
    fill=mkt["fill"],
    our_ask_ref=mkt["our_ask_ref"],
)

# ----------------- send orders -----------------

for sid, amount in charge_orders:
    if amount > 0:
        psm.orders.charge(sid, amount)

for sid, amount in discharge_orders:
    if amount > 0:
        psm.orders.discharge(sid, amount)

for volume, price in ladder:
    psm.orders.sell(volume, price)

# ----------------- debug -----------------

print("NOW GEN:", round(current_generation, 3))
print("NOW CONS:", round(current_consumption, 3))
print("NEXT GEN:", round(generation_next, 3))
print("NEXT CONS:", round(consumption_next, 3))
print("BALANCE NEXT:", round(balance_next, 3))
print("RESERVE:", round(reserve, 3))

print("MARKET REF:", round(mkt["market_ref"], 3))
print("OUR ASK REF:", round(mkt["our_ask_ref"], 3))
print("FILL:", round(mkt["fill"], 3))

print("SOC:", round(total_soc, 3), "/", round(capacity_total, 3), "TARGET:", round(target_soc, 3))
print("CHARGE:", charge_orders)
print("DISCHARGE:", discharge_orders)
print("SELL VOLUME:", sell_volume)
print("LADDER:", ladder)

for index, net in psm.networks.items():
    print("== Энергорайон", index, "==")
    print("Адрес:", net.location)
    print("Генерация:", net.upflow)
    print("Потребление:", net.downflow)
    print("Потери:", net.losses)

print(psm.orders.humanize())
psm.save_and_exit()
