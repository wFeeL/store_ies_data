import sys
import json
import traceback
import math
import urllib.request
from collections import namedtuple, UserList
from argparse import Namespace
from copy import deepcopy

__all__ = [
    "Powerstand", "Object", "Line", "Powerline",
    "Historic", "Receipt", "ExchangeReceipt"
]

def pretty_bool(v):
    return 'вкл.' if v else 'выкл.'

def pretty_agent(ag):
    return f'{ag["place"]}.{ag["player"]}'

def safe_tail(data):
    if len(data):
        return None
    return data[-1]

def safe_head(data):
    if len(data):
        return None
    return data[0]

Historic = namedtuple("Historic", ("now", "then"))
Historic.__str__ = lambda self: f"{self.now} (было {safe_tail(self.then)})"

Receipt = namedtuple("Receipt", ("income", "loss"))
Receipt.__str__ = lambda self: f"(+{self.income} ₽, -{self.loss} ₽)"
Receipt.__add__ = lambda self, x: __add_receipt(self, x)

def __add_receipt(self, x):
    if isinstance(x, Receipt):
        return Receipt(self.income + x.income, self.loss + x.loss)
    raise TypeError(x)

ExchangeReceipt = namedtuple("ExchangeReceipt", ("askedAmount", "askedPrice", "contractedAmount", "contractedPrice", "instantAmount"))
ExchangeReceipt.__str__ = \
    lambda self: f"[контракт: " \
                 f"запрос {self.askedAmount:.2f} МВт по {self.askedPrice} ₽/МВт, " \
                 f"факт {self.contractedAmount:.2f} МВт по {self.contractedPrice:.2f} ₽/МВт, " \
                 f"РММ {self.instantAmount:.2f} МВт]"

Forecasts = namedtuple("Forecasts", ("hospital", "factory", "office", "houseA", "houseB", "sun", "wind"))

TotalPower = namedtuple("TotalPower", ("generated", "consumed", "external", "losses"))

Power = namedtuple("Line", ("generated", "consumed"))
Power.__str__ = lambda self: f"(+{self.generated} МВт⋅ч -{self.consumed} МВт⋅ч)"
Power.total = lambda self: self.generated - self.consumed

Object = namedtuple("Object", ("id", "type", "contract", "address", "path", "failed",
                               "charge", "score", "power", "windRotation"))
Object.__str__ = lambda self: f"{self.type} ({self.power.now}, {self.score.now})"

Line = namedtuple("Line", ("id", "line"))
Line.__str__ = lambda self: f"{self.id}-{self.line}"

Powerline = namedtuple("Powerline", ("location", "upflow", "downflow", "losses", "id"))
Powerline.__str__ = lambda self: f"{self.location} ({pretty_bool(self.online)})"

station_types = {"miniA", "miniB", "main"}
storage_types = {"storage"}

def make_objectid(x):
    l = x["load"]
    l = l[0].lower() + l[1:]
    return (l, x["int"])

def make_line(l):
    return Line(make_objectid(l["id"]), l["line"])

def make_historic(d, fn):
    return Historic(fn(**d["now"]), [fn(**x) for x in d["then"][::-1]])

def make_historicM(d, fn):
    return make_historic(d, fn) if d else None

def make_historic_(d, fn):
    return Historic(fn(d["now"]), [fn(x) for x in d["then"][::-1]])

def make_historicM_(d, fn):
    return make_historic_(d, fn) if d else None


def make_object(d, stations, storages):
    obj = Object(
        id=make_objectid(d["id"]),
        address=tuple(d["address"]),
        contract=d["contract"],
        path=tuple(tuple(make_line(l) for l in a) for a in d["path"]),
        score=make_historic(d["score"], Receipt),
        power=make_historic(d["power"], Power),
        charge=make_historicM_(d["charge"], float),
        windRotation=make_historicM_(d["windRotation"], float),
        failed=d["failed"],
        type=d["class"],
    )
    if obj.type in station_types:
        stations[obj.address[0]] = obj.id
        x, y = stations[obj.address[0]]
        stations[obj.address[0]] = {"load": x[0].upper() + x[1:], "int": y}
    if obj.type in storage_types:
        storages[obj.address[0]] = obj.id
    return obj


def make_powerline(d):
    return Powerline(
        id=d["id"],
        location=tuple(make_line(l) for l in d["location"]),
        upflow=float(d["upflow"]["now"]),
        downflow=float(d["downflow"]["now"]),
        losses=float(d["losses"]["now"]),
    )


def from_chipping(d):
    return Historic(d["current"], d["done"][::-1])


class ForecastSet(UserList):
    def __init__(self, *args, spread):
        super().__init__(*args)
        self.spread = spread
    def __getitem__(self, i):
        if isinstance(i, slice):
            return ForecastSet(self.data[i], spread=self.spread)
        else:
            return self.data[i]

def make_forecast_set(d):
    return ForecastSet(tuple(d["forecast"]["values"]),
                       spread=d["spread"])

def make_marked_forecast_set(ds):
    return {
        d["mark"] : ForecastSet(tuple(d["forecast"]["values"]),
                                spread=d["spread"])
        for d in ds
    }

class Powerstand:

    GRAPH_COUNT = 4

    def __init__(self, data, offline=True, bloat_fields=False):

        if data['tag'] != "CoreNTO11":
            raise ValueError(f"несовместимая версия фреймворка ({data['tag']})")
        data = data['data']['contents']['cargo']

        self.__offline = offline
        self.__owner = data['scores'][0][0]
        self.__orders = orders = []
        self.__station_index = dict()
        self.__storage_index = dict()
        self.raw_data = data  # NOTE: deepcopy не делается, потому что долго и бесполезно

        # ИНВАРИАНТ: приходит состояние, подчищенное для конкретного игрока

        self.config = data['conf']
        self.tick = data['tick']
        self.gameLength = self.config['gameLength']
        self.scoreDelta = Receipt(**data["scores"][0][1]["now"]["total"])

        # hello there 👀 👀 👀 
        self.wind = from_chipping(data['weatherWind'])
        self.sun = from_chipping(data['weatherSun'])
        
        self.objects = [make_object(obj, self.__station_index, self.__storage_index) 
                        for obj in data["objs"]]
        self.networks = {i+1: make_powerline(pl) for (i, pl) in enumerate(data["nets"])}
        raw_fc = data["forecasts"]
        self.forecasts = Forecasts(
            make_forecast_set(raw_fc["hospital"]),
            make_forecast_set(raw_fc["factory"]),
            make_forecast_set(raw_fc["office"]),
            make_forecast_set(raw_fc["houseA"]),
            make_forecast_set(raw_fc["houseB"]),
            make_forecast_set(raw_fc["sun"]),
            make_forecast_set(raw_fc["wind"]),
        )

        self.exchange = [ExchangeReceipt(d["askedAmount"], d["askedPrice"], 
                                         d["contractedAmount"], d["contractedPrice"],
                                         d["instantAmount"])
                         for d in data["exchangeReceipts"]]
        self.exchangeLog = [d for d in data["exchangeLog"]]

        raw_tp = data["totalPowers"][0][1]["now"]
        self.total_power = TotalPower(raw_tp["totalGenerated"], raw_tp["totalConsumed"],
                                      raw_tp["totalFromExternal"], raw_tp["totalLost"])

        if bloat_fields:
            self.scoreTotal = sum(map(lambda x: Receipt(**x["total"]),
                                  data["scores"][0][1]["then"]),
                                  self.scoreDelta)
            self.topo = {c.location: i for (i, c) in self.networks.items()}

        self.orders = Namespace(
            charge=lambda address, power: self.__change_cell(address, power, True),
            discharge=lambda address, power: self.__change_cell(address, power, False),
            sell=lambda amount, price: self.__outstanding(amount, price, True),
            # debug functions
            get=lambda: orders.copy(),
            humanize=lambda: self.__humanize_orders(),
        )

    def __change_cell(self, address, power, charge=True):
        try:
            power = float(power)
            if power < 0:
                self.__warn_tb("Отрицательное значение энергии в приказе на аккумулятор. "
                               "Приказ не принят.", cut=3)
                return
        except ValueError:
            self.__warn_tb("Для приказа на аккумулятор нужен float-совместимый "
                           "тип. Приказ не принят.", cut=3)
            return
        if address not in self.__storage_index:
            self.__warn_tb("Такого накопителя/подстанции не существует. "
                           "Приказ не принят.", cut=3)
            return
        # TODO? ограничение сверху
        order = "charge" if charge else "discharge"
        self.__orders.append({"orderT": order, "address": address, "power": power})

    def __outstanding(self, amount, price, sell=True):
        try:
            amount = float(amount)
            if amount < 0:
                self.__warn_tb("Неположительное значение энергии в заявке на биржу. "
                               "Приказ не принят.", cut=3)
                return
        except ValueError:
            self.__warn_tb("Для заявки на биржу нужно float-совместимое "
                           "значение энергии. Приказ не принят.", cut=3)
            return
        try:
            price = float(price)
            if price < 0:
                self.__warn_tb("Неположительное значение стоимости в заявке на биржу. "
                               "Приказ не принят.", cut=3)
                return
        except ValueError:
            self.__warn_tb("Для заявки на биржу нужно float-совместимое "
                           "значение стоимости. Приказ не принят.", cut=3)
            return
        # TODO? ограничение сверху
        order = "sell" if sell else "buy"
        self.__orders.append({"orderT": order, "amount": amount, "price": price})

    def __commit(self):
        if self.__offline:
            print("<<< И тут приказы отправляются в систему... >>>")
            return 0
        package = {
            "tag": "VariantOrders_NTO11", 
            "contents": [
                { 
                    "owner": self.__owner,
                    "order": o,
                }
                for o in self.__orders
            ],
        }
        data = json.dumps(package).encode()
        request = urllib.request.urlopen("http://localhost:26000/orders", data=data)
        if request.getcode() != 200:
            print(package)
            raise ConnectionRefusedError("Couldn't send data to server")
        return 0

    def get_orders(self):
        return self.__humanize_orders()

    def get_user_data(self):
        return deepcopy(self.__user_data)

    def save_and_exit(self):
        sys.exit(self.__commit())

    @staticmethod
    def safe_float(v):  # TODO: изменить cut в warn_tb
        try:
            v = float(v)
            if not math.isfinite(v):
                Powerstand.__warn_tb("Неконечное число в графике. "
                                     "Заменено на 0.", cut=5)
                return 0
            return v
        except ValueError:
            Powerstand.__warn_tb("Несовместимое с float значение в графике. "
                                 "Заменено на 0.", cut=5)
            return 0

    @staticmethod
    def __warn_tb(error, warning=False, cut=2):
        level = "Предупреждение" if warning else "Ошибка"
        print("".join(traceback.format_list(traceback.extract_stack()[:-cut])) +
              f"{level}: {error}", file=sys.stderr, flush=True)

    def __humanize_orders(self):
        return [self.humanize_order(o) for o in self.__orders]

    @staticmethod
    def humanize_order(order):
        type = order["orderT"]
        if type == "sell":
            return f"заявка на продажу {order['amount']:.2f} МВт⋅ч за {order['price']:.2f} ₽"
        if type == "charge":
            return f"зарядка аккумуляторов {order['address']} на {order['power']:.2f} МВт⋅ч"
        if type == "discharge":
            return f"разрядка аккумуляторов {order['address']} на {order['power']:.2f} МВт⋅ч"
        else:
            return "неизвестный приказ"
