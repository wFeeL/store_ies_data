import json
import os
import ips


# =========================
# НАСТРОЙКИ
# =========================

MODE = "live"
# MODE:
#   "live" -> реальный стенд: ips.init()
#   "test" -> локальный вшитый пример: ips.init_test()
#   "file" -> локальный JSON-файл: ips.from_file("snapshot_source.json")

SOURCE_FILE = "snapshot_source.json"
OUT_DIR = os.path.expanduser("~/ips3-sandbox")


# =========================
# ИНИЦИАЛИЗАЦИЯ
# =========================

def init_psm():
    if MODE == "live":
        return ips.init()
    if MODE == "test":
        return ips.init_test()
    if MODE == "file":
        return ips.from_file(SOURCE_FILE)
    raise ValueError(f"Unknown MODE: {MODE}")


# =========================
# СЕРИАЛИЗАЦИЯ
# =========================

def is_primitive(x):
    return x is None or isinstance(x, (bool, int, float, str))


def to_jsonable(obj, depth=0, max_depth=8):
    if depth > max_depth:
        return "<max_depth>"

    if is_primitive(obj):
        return obj

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[str(k)] = to_jsonable(v, depth + 1, max_depth)
        return out

    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(v, depth + 1, max_depth) for v in obj]

    # Частый случай: объекты библиотеки имеют публичные поля
    out = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        out[name] = to_jsonable(value, depth + 1, max_depth)
    return out


# =========================
# СБОР СНИМКА
# =========================

def build_snapshot(psm):
    snapshot = {
        "tick": getattr(psm, "tick", None),
        "gameLength": getattr(psm, "gameLength", None),
        "scoreDelta": getattr(psm, "scoreDelta", None),
        "wind": to_jsonable(getattr(psm, "wind", None)),
        "sun": to_jsonable(getattr(psm, "sun", None)),
        "total_power": to_jsonable(getattr(psm, "total_power", None)),
        "objects": to_jsonable(getattr(psm, "objects", None)),
        "networks": to_jsonable(getattr(psm, "networks", None)),
        "forecasts": to_jsonable(getattr(psm, "forecasts", None)),
        "exchange": to_jsonable(getattr(psm, "exchange", None)),
        "exchangeLog": to_jsonable(getattr(psm, "exchangeLog", None)),
        "config": to_jsonable(getattr(psm, "config", None)),
    }
    return snapshot


# =========================
# СОХРАНЕНИЕ
# =========================

def save_snapshot(snapshot):
    tick = snapshot.get("tick", "unknown")
    filename = os.path.join(OUT_DIR, f"tick_{tick}_snapshot.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    return filename


# =========================
# MAIN
# =========================

def main():
    psm = init_psm()

    try:
        snapshot = build_snapshot(psm)
        filename = save_snapshot(snapshot)

        print(f"Snapshot saved: {filename}")
        print(f"Current tick: {snapshot.get('tick')}")
        if snapshot.get("tick") != 0:
            print("WARNING: это не нулевой такт.")
    finally:
        psm.save_and_exit()


if __name__ == "__main__":
    main()
