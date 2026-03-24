import ips
import math

psm = ips.init()
# psm = ips.from_file("/home/player/test.json")
# psm = ips.from_log("sbox-1.log", 10)

print("TICK", psm.tick)

""" 
Очень базовый скрипт для стенда ИЭС.
Он наивно предсказывает генерацию по СЭC/ВЭС
И ничего больше не делает.
"""

past_tick = max(psm.tick - 1, 0)
next_tick = min(psm.tick + 1, len(psm.forecasts.houseA) - 1)

now_wind = psm.wind.now
next_wind = psm.wind.then[0]

past_sun = psm.forecasts.sun[past_tick]
now_sun = psm.forecasts.sun[psm.tick]
next_sun = psm.forecasts.sun[next_tick]

consumption = 0  # прогноз суммарного потребления
generation = 0  # прогноз суммарной генерации

for obj in psm.objects:
    addr = obj.address[0]
    if obj.type == "wind":
        # вычисляем прогноз ветра
        if now_wind <= next_wind:
            generation += obj.power.now.generated * 1.10
        else: # now_wind > next_wind
            generation += obj.power.now.generated * 0.85
        continue
    if obj.type == "solar":
        # вычисляем прогноз солнца
        if now_sun <= next_sun:
            generation += obj.power.now.generated * 1.05
        else: # now_sun > next_sun
            generation += obj.power.now.generated * 0.85
        continue
    # вычисляем прогноз потребления
    if obj.type == "housea":
        consumption += psm.forecasts.houseA[next_tick]
    if obj.type == "houseb":
        consumption += psm.forecasts.houseB[next_tick]
    if obj.type == "factory":
        consumption += psm.forecasts.factory[next_tick]
    if obj.type == "office":
        consumption += psm.forecasts.office[next_tick]
    if obj.type == "hospital":
        consumption += psm.forecasts.hospital[next_tick]

shortage = consumption - generation

print("SHORT", shortage)

for index, net in psm.networks.items():
    print("== Энергорайон", index, "==")
    print("Адрес:", net.location)
          # [ (ID подстанции, № линии) ]
    print("Генерация:", net.upflow) # float
    print("Потребление:", net.downflow) # float
    print("Потери:", net.losses) # float

print(psm.orders.humanize())
psm.save_and_exit()
