import csv
import glob
import json
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

import ips

os.environ["IES_FORECAST_FILE"] = os.path.expanduser("~/ips3-sandbox/forecast.csv")

STATE_FILE = os.path.expanduser('~/ips3-sandbox/ies_state.json')
LOG_DIR = os.path.expanduser('~/ips3-sandbox/ies_logs')
TICK_SUMMARY_FILE = os.path.join(LOG_DIR, 'tick_summary.jsonl')
STRATEGY_DEBUG_FILE = os.path.join(LOG_DIR, 'strategy_debug.jsonl')
OBJECTS_CSV = os.path.join(LOG_DIR, 'objects_timeseries.csv')
NETWORKS_CSV = os.path.join(LOG_DIR, 'networks_timeseries.csv')
EXCHANGE_CSV = os.path.join(LOG_DIR, 'exchange_timeseries.csv')
FORECAST_ERRORS_CSV = os.path.join(LOG_DIR, 'forecast_errors.csv')
DERIVED_METRICS_CSV = os.path.join(LOG_DIR, 'derived_metrics.csv')
OBJECT_PREDICTIONS_CSV = os.path.join(LOG_DIR, 'object_predictions.csv')

MIN_ORDER_VOLUME = 0.25
MIN_RESERVE = 0.8
SOC_FLOOR_FRAC = 0.06
SOC_CEIL_FRAC = 0.95
ENDGAME_TICKS = 5
LOOKAHEAD = 6
STRICT_FIRST_TICK_CAP = True

SOLAR_SEED_FACTORS = {
    's1': 0.44,
    's7': 0.77,
    's8': 0.39,
}
WIND_SEED_FACTORS = {
    'a3': 0.0041,
    'a5': 0.0058,
    'a6': 0.0054,
    'a8': 0.0047,
}
LOAD_BIAS_PRIOR = 0.60
LOAD_TYPE_BIAS_PRIORS = {
    'factory': 0.40,
    'houseA': 0.40,
    'houseB': 0.52,
    'office': 0.60,
    'hospital': 0.70,
}
LOSS_MODEL_PRIOR = {
    'gen_quad': 0.0058,
    'gen_lin': 0.2120,
    'load_quad': 0.0103,
    'load_lin': -0.0687,
    'base': 0.07,
}

FORECAST_INDEX_TO_NAME = {
    0: 'hospital',
    1: 'factory',
    2: 'office',
    3: 'houseA',
    4: 'houseB',
    5: 'sun',
    6: 'wind',
}
OBJECT_TYPE_TO_FORECAST = {
    'hospital': 'hospital',
    'factory': 'factory',
    'office': 'office',
    'houseA': 'houseA',
    'houseB': 'houseB',
}
FORECAST_HEADER_TO_KEY = {
    'ветер': 'wind',
    'солнце': 'sun',
    'больницы': 'hospital',
    'заводы': 'factory',
    'офисы': 'office',
    'дома а': 'houseA',
    'дома a': 'houseA',
    'дом а': 'houseA',
    'house a': 'houseA',
    'housea': 'houseA',
    'дома б': 'houseB',
    'дома b': 'houseB',
    'дом б': 'houseB',
    'house b': 'houseB',
    'houseb': 'houseB',
    'hospital': 'hospital',
    'factory': 'factory',
    'office': 'office',
    'sun': 'sun',
    'wind': 'wind',
}
FORECAST_SERIES_ORDER = ('hospital', 'factory', 'office', 'houseA', 'houseB', 'sun', 'wind')
_FORECAST_CACHE: Dict[str, Any] = {'key': None, 'payload': None}


# =====================
# Generic helpers
# =====================

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def round_vol(x: float) -> float:
    return round(max(0.0, float(x)), 3)


def round_price(x: float, price_max: float = 20.0, price_step: float = 0.2) -> float:
    step = max(0.01, float(price_step))
    hi = max(2.0, float(price_max))
    x = clamp(float(x), 2.0, hi)
    return round(round(x / step) * step, 2)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return None if default is None else float(default)
        return float(value)
    except Exception:
        return None if default is None else float(default)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def addr_to_str(address: Any) -> str:
    if isinstance(address, list):
        return '|'.join(str(x) for x in address)
    if isinstance(address, tuple):
        return '|'.join(str(x) for x in address)
    return str(address)


def ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def write_jsonl(path: str, row: Dict[str, Any]) -> None:
    try:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    except Exception:
        pass


def append_csv(path: str, fieldnames: List[str], row: Dict[str, Any]) -> None:
    try:
        file_exists = os.path.exists(path)
        with open(path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore', restval='')
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception:
        pass


def reset_runtime_outputs() -> None:
    ensure_dir(LOG_DIR)
    for path in [
        TICK_SUMMARY_FILE,
        STRATEGY_DEBUG_FILE,
        OBJECTS_CSV,
        NETWORKS_CSV,
        EXCHANGE_CSV,
        FORECAST_ERRORS_CSV,
        DERIVED_METRICS_CSV,
        OBJECT_PREDICTIONS_CSV,
    ]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


# =====================
# Forecast helpers
# =====================

def normalize_forecast_header(name: Any) -> str:
    s = str(name or '').strip().lower().replace('ё', 'е').replace('_', ' ').replace('-', ' ')
    s = ' '.join(s.split())
    return FORECAST_HEADER_TO_KEY.get(s, '')


def _forecast_file_rank(path: str) -> Tuple[int, int, float]:
    name = os.path.basename(path).lower()
    forecast_like = int('forecast' in name)
    return (forecast_like, int(name.startswith('forecast')), os.path.getmtime(path))


def _parse_forecast_csv(path: str) -> Optional[Dict[str, Any]]:
    series = {name: [] for name in FORECAST_SERIES_ORDER}
    rows_read = 0
    try:
        with open(path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return None
            header_map = {field: normalize_forecast_header(field) for field in reader.fieldnames}
            recognized = {canon for canon in header_map.values() if canon}
            if not {'wind', 'sun'}.issubset(recognized):
                return None
            for row in reader:
                if not any((str(v).strip() for v in row.values())):
                    continue
                rows_read += 1
                normalized: Dict[str, float] = {}
                for raw_key, value in row.items():
                    canon = header_map.get(raw_key, '')
                    if canon:
                        normalized[canon] = safe_float(value, 0.0)
                for name in FORECAST_SERIES_ORDER:
                    if name in normalized:
                        series[name].append(normalized[name])
    except Exception:
        return None
    max_len = max((len(v) for v in series.values()), default=0)
    if max_len <= 0:
        return None
    return {
        'path': path,
        'rows': rows_read,
        'bundle': {
            name: {'data': list(values), 'spread': 0.0}
            for name, values in series.items()
            if values
        },
    }


def find_external_forecast_file() -> Optional[str]:
    env_path = os.environ.get('IES_FORECAST_FILE')
    if env_path:
        path = os.path.abspath(os.path.expanduser(env_path))
        return path if _parse_forecast_csv(path) is not None else None

    candidates: List[str] = []
    search_roots = [
        os.getcwd(),
        os.path.expanduser('~/ips3-sandbox'),
        os.path.dirname(os.path.abspath(__file__)),
        '/mnt/data',
    ]
    seen = set()
    for root in search_roots:
        if not root or root in seen or not os.path.isdir(root):
            continue
        seen.add(root)
        candidates.extend(sorted(glob.glob(os.path.join(root, 'forecast*.csv'))))
        # broader scan but only validated forecast files will pass
        candidates.extend(sorted(glob.glob(os.path.join(root, '*.csv'))))

    unique_candidates: List[str] = []
    seen_paths = set()
    for p in candidates:
        ap = os.path.abspath(p)
        if ap in seen_paths or not os.path.isfile(ap):
            continue
        seen_paths.add(ap)
        unique_candidates.append(ap)

    unique_candidates.sort(key=_forecast_file_rank, reverse=True)
    for path in unique_candidates:
        if _parse_forecast_csv(path) is not None:
            return path
    return None


def load_external_forecast_csv() -> Optional[Dict[str, Any]]:
    path = find_external_forecast_file()
    if not path:
        return None
    try:
        stat = os.stat(path)
        cache_key = (path, stat.st_mtime_ns, stat.st_size)
    except Exception:
        cache_key = (path, None, None)

    if _FORECAST_CACHE.get('key') == cache_key:
        return _FORECAST_CACHE.get('payload')

    payload = _parse_forecast_csv(path)
    _FORECAST_CACHE['key'] = cache_key
    _FORECAST_CACHE['payload'] = payload
    return payload


# =====================
# Access layer for live psm objects and compact JSON snapshots
# =====================

def is_compact_object(obj: Any) -> bool:
    return isinstance(obj, list)


def get_tick(psm: Any) -> int:
    if isinstance(psm, dict):
        return safe_int(psm.get('tick', 0))
    return safe_int(getattr(psm, 'tick', 0))


def get_game_length(psm: Any) -> int:
    if isinstance(psm, dict):
        return safe_int(psm.get('gameLength', 100), 100)
    return safe_int(getattr(psm, 'gameLength', 100), 100)


def get_score_breakdown(psm: Any) -> Tuple[float, float, Optional[float]]:
    raw = psm.get('scoreDelta') if isinstance(psm, dict) else getattr(psm, 'scoreDelta', None)
    if isinstance(raw, list):
        if len(raw) > 1:
            return safe_float(raw[0], 0.0), 0.0, safe_float(raw[1], 0.0)
        return safe_float(raw[0], 0.0) if raw else 0.0, 0.0, None
    income = safe_float(getattr(raw, 'income', None), None)
    loss = safe_float(getattr(raw, 'loss', None), None)
    if income is not None or loss is not None:
        return safe_float(income, 0.0), safe_float(loss, 0.0), None
    delta = safe_float(raw, 0.0)
    return delta, 0.0, None


def get_score_delta(psm: Any) -> float:
    income, loss, _ = get_score_breakdown(psm)
    return income - loss


def get_total_score(psm: Any) -> Optional[float]:
    _, _, total = get_score_breakdown(psm)
    return total


def get_total_power_tuple(psm: Any) -> Tuple[float, float, float, float]:
    raw = psm.get('total_power') if isinstance(psm, dict) else getattr(psm, 'total_power', None)
    if isinstance(raw, list):
        return (
            safe_float(raw[0], 0.0),
            safe_float(raw[1], 0.0),
            safe_float(raw[2], 0.0),
            safe_float(raw[3], 0.0),
        )
    return (
        safe_float(getattr(raw, 'generated', 0.0), 0.0),
        safe_float(getattr(raw, 'consumed', 0.0), 0.0),
        safe_float(getattr(raw, 'external', 0.0), 0.0),
        safe_float(getattr(raw, 'losses', 0.0), 0.0),
    )


def get_weather_now(psm: Any, name: str) -> float:
    raw = psm.get(name) if isinstance(psm, dict) else getattr(psm, name, None)
    if isinstance(raw, list):
        return safe_float(raw[0], 0.0)
    return safe_float(getattr(raw, 'now', 0.0), 0.0)


def get_forecast_bundle(psm: Any) -> Dict[str, Dict[str, Any]]:
    # Prefer real psm.forecasts if available, else explicit CSV fallback.
    raw = psm.get('forecasts') if isinstance(psm, dict) else getattr(psm, 'forecasts', None)
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            name = FORECAST_INDEX_TO_NAME.get(idx, f'f{idx}')
            out[name] = {
                'data': list(item.get('data', [])),
                'spread': safe_float(item.get('spread', 0.0), 0.0),
                'source': 'psm',
            }
    else:
        for name in FORECAST_SERIES_ORDER:
            seq = getattr(raw, name, None) if raw is not None else None
            if seq is None:
                continue
            out[name] = {
                'data': list(seq),
                'spread': safe_float(getattr(seq, 'spread', 0.0), 0.0),
                'source': 'psm',
            }

    psm_rows = max((len(v.get('data', [])) for v in out.values() if isinstance(v, dict) and 'data' in v), default=0)
    if psm_rows > 0:
        out['_meta'] = {'source': 'psm', 'path': None, 'rows': psm_rows}
        return out

    ext = load_external_forecast_csv()
    if ext:
        out = {}
        for name, item in ext['bundle'].items():
            out[name] = {'data': list(item.get('data', [])), 'spread': safe_float(item.get('spread', 0.0), 0.0), 'source': 'csv'}
        out['_meta'] = {'source': 'csv', 'path': ext['path'], 'rows': ext['rows']}
        return out

    out['_meta'] = {'source': 'psm', 'path': None, 'rows': 0}
    return out


def get_object_list(psm: Any) -> List[Any]:
    if isinstance(psm, dict):
        return list(psm.get('objects', []))
    return list(getattr(psm, 'objects', []))


def get_network_items(psm: Any) -> List[Tuple[str, Any]]:
    raw = psm.get('networks') if isinstance(psm, dict) else getattr(psm, 'networks', {})
    return list(raw.items())


def get_exchange_list(psm: Any) -> List[Any]:
    if isinstance(psm, dict):
        return list(psm.get('exchange', []))
    return list(getattr(psm, 'exchange', []))


def get_exchange_log(psm: Any) -> List[float]:
    if isinstance(psm, dict):
        return list(psm.get('exchangeLog', []))
    return list(getattr(psm, 'exchangeLog', []))


def get_config_dict(psm: Any) -> Dict[str, Any]:
    if isinstance(psm, dict):
        return dict(psm.get('config', {}))
    cfg = getattr(psm, 'config', None)
    if isinstance(cfg, dict):
        return dict(cfg)
    out: Dict[str, Any] = {}
    if cfg is None:
        return out
    for key in dir(cfg):
        if key.startswith('_'):
            continue
        try:
            value = getattr(cfg, key)
        except Exception:
            continue
        if callable(value):
            continue
        out[key] = value
    return out


def obj_id(obj: Any) -> Any:
    if is_compact_object(obj):
        return obj[0]
    return getattr(obj, 'id', None)


def obj_type(obj: Any) -> str:
    if is_compact_object(obj):
        return str(obj[1])
    return str(getattr(obj, 'type', ''))


def obj_contract(obj: Any) -> float:
    if is_compact_object(obj):
        return safe_float(obj[2], 0.0)
    return safe_float(getattr(obj, 'contract', 0.0), 0.0)


def obj_address(obj: Any) -> List[str]:
    if is_compact_object(obj):
        return list(obj[3])
    return list(getattr(obj, 'address', []))


def obj_address_key(obj: Any) -> str:
    return addr_to_str(obj_address(obj))


def obj_path(obj: Any) -> Any:
    if is_compact_object(obj):
        return obj[4]
    return getattr(obj, 'path', [])


def obj_score_now(obj: Any) -> Tuple[float, float]:
    if is_compact_object(obj):
        raw = obj[7][0] if len(obj) > 7 and obj[7] else [0.0, 0.0]
        return safe_float(raw[0], 0.0), safe_float(raw[1], 0.0)
    score_now = getattr(getattr(obj, 'score', None), 'now', None)
    return safe_float(getattr(score_now, 'income', 0.0), 0.0), safe_float(getattr(score_now, 'loss', 0.0), 0.0)


def obj_power_now(obj: Any) -> Tuple[float, float]:
    if is_compact_object(obj):
        raw = obj[8][0] if len(obj) > 8 and obj[8] else [0.0, 0.0]
        return safe_float(raw[0], 0.0), safe_float(raw[1], 0.0)
    power_now = getattr(getattr(obj, 'power', None), 'now', None)
    return safe_float(getattr(power_now, 'generated', 0.0), 0.0), safe_float(getattr(power_now, 'consumed', 0.0), 0.0)


def obj_charge_now(obj: Any) -> Optional[float]:
    if is_compact_object(obj):
        if len(obj) > 6 and isinstance(obj[6], list) and obj[6]:
            return safe_float(obj[6][0], 0.0)
        return None
    ch = getattr(obj, 'charge', None)
    if ch is None:
        return None
    return safe_float(getattr(ch, 'now', 0.0), 0.0)


def obj_wind_rotation_now(obj: Any) -> Optional[float]:
    if is_compact_object(obj):
        if len(obj) > 9 and isinstance(obj[9], list) and obj[9]:
            return safe_float(obj[9][0], 0.0)
        return None
    wr = getattr(obj, 'windRotation', None)
    if wr is None:
        return None
    return safe_float(getattr(wr, 'now', 0.0), 0.0)


def obj_failed(obj: Any) -> int:
    if is_compact_object(obj):
        if len(obj) > 5:
            return safe_int(obj[5], 0)
        return 0
    return safe_int(getattr(obj, 'failed', 0), 0)


def net_location(net: Any) -> Any:
    if isinstance(net, list):
        return net[0]
    return getattr(net, 'location', [])


def net_upflow(net: Any) -> float:
    if isinstance(net, list):
        return safe_float(net[1], 0.0)
    return safe_float(getattr(net, 'upflow', 0.0), 0.0)


def net_downflow(net: Any) -> float:
    if isinstance(net, list):
        return safe_float(net[2], 0.0)
    return safe_float(getattr(net, 'downflow', 0.0), 0.0)


def net_losses(net: Any) -> float:
    if isinstance(net, list):
        return safe_float(net[3], 0.0)
    return safe_float(getattr(net, 'losses', 0.0), 0.0)


def exchange_receipt_data(receipt: Any) -> Dict[str, float]:
    if isinstance(receipt, list):
        asked = safe_float(receipt[0], 0.0)
        out = {
            'askedAmount': asked,
            'askedPrice': safe_float(receipt[1], 0.0),
            'contractedAmount': safe_float(receipt[2], 0.0),
            'contractedPrice': safe_float(receipt[3], 0.0),
            'instantAmount': safe_float(receipt[4], 0.0),
        }
    else:
        asked = safe_float(getattr(receipt, 'askedAmount', 0.0), 0.0)
        out = {
            'askedAmount': asked,
            'askedPrice': safe_float(getattr(receipt, 'askedPrice', 0.0), 0.0),
            'contractedAmount': safe_float(getattr(receipt, 'contractedAmount', 0.0), 0.0),
            'contractedPrice': safe_float(getattr(receipt, 'contractedPrice', 0.0), 0.0),
            'instantAmount': safe_float(getattr(receipt, 'instantAmount', 0.0), 0.0),
        }
    out['side'] = 'buy' if asked > 0 else ('sell' if asked < 0 else 'flat')
    return out


# =====================
# State
# =====================

def default_state() -> Dict[str, Any]:
    return {
        'prev_useful_supply_est': None,
        'last_sell_volume': 0.0,
        'abs_err_ewma': 1.2,
        'loss_ratio_ewma': 0.18,
        'fill_ratio_ewma': 0.70,
        'market_ref': 7.2,
        'load_bias_total': LOAD_BIAS_PRIOR,
        'load_abs_err': 2.0,
        'object_models': {},
        'weather_history': {'wind': [], 'sun': []},
        'loss_model': dict(LOSS_MODEL_PRIOR, scale=1.0),
        'object_cum': {},
        'cumulative': {
            'solar_generated': 0.0,
            'wind_generated': 0.0,
            'gross_generated': 0.0,
            'gross_consumed': 0.0,
            'losses': 0.0,
            'useful_energy': 0.0,
            'buy_asked': 0.0,
            'buy_contracted': 0.0,
            'sell_asked': 0.0,
            'sell_contracted': 0.0,
            'instant_abs': 0.0,
        },
    }


def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            st = default_state()
            st.update(data)
            return st
    except Exception:
        pass
    return default_state()


def save_state(state: Dict[str, Any]) -> None:
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _model_key(address: str, kind: str) -> str:
    return f'{kind}:{address}'


def get_model(state: Dict[str, Any], key: str, kind: str) -> Dict[str, Any]:
    models = state.setdefault('object_models', {})
    mkey = _model_key(key, kind)
    if mkey not in models:
        if kind == 'solar':
            models[mkey] = {
                'kind': kind,
                'factor': SOLAR_SEED_FACTORS.get(key, 0.65),
                'err': 0.8,
            }
        elif kind == 'wind':
            models[mkey] = {
                'kind': kind,
                'factor': WIND_SEED_FACTORS.get(key, 0.0050),
                'rot_factor': 80.0,
                'wind_to_rot': 0.040,
                'rot_curve': {},
                'max_power_seen': 0.0,
                'err': 2.5,
                'last_failed': 0,
            }
        else:
            models[mkey] = {'kind': kind, 'bias': 1.0, 'err': 0.6}
    return models[mkey]


def update_wind_rot_curve(model: Dict[str, Any], rotation_now: float, actual_power: float) -> None:
    if rotation_now <= 0.03 or actual_power < 0.0:
        return
    curve = model.setdefault('rot_curve', {})
    bucket = round(rotation_now / 0.05) * 0.05
    key = f'{bucket:.2f}'
    prev = safe_float(curve.get(key, 0.0), 0.0)
    curve[key] = max(prev, actual_power)
    if len(curve) > 80:
        keys = sorted(curve.keys(), key=lambda k: float(k))
        for old in keys[:-80]:
            curve.pop(old, None)


def estimate_wind_from_curve(model: Dict[str, Any], rotation: float) -> Optional[float]:
    curve = model.get('rot_curve') or {}
    if rotation <= 0.03 or len(curve) < 3:
        return None
    pts = sorted((safe_float(k, 0.0), safe_float(v, 0.0)) for k, v in curve.items())
    close = [(r, p) for r, p in pts if abs(r - rotation) <= 0.18]
    if not close:
        close = sorted(pts, key=lambda rp: abs(rp[0] - rotation))[:4]
    if not close:
        return None
    num = 0.0
    den = 0.0
    near_max = 0.0
    for r, p in close:
        d = abs(r - rotation)
        w = 1.0 / max(d, 0.03)
        num += w * p
        den += w
        if d <= 0.08:
            near_max = max(near_max, p)
    est = num / max(den, 1e-9)
    if near_max > 0.0:
        est = max(est, 0.92 * near_max)
    return max(0.0, est)


# =====================
# Analytics and logging
# =====================

def extract_object_rows(psm: Any) -> List[Dict[str, Any]]:
    rows = []
    for obj in get_object_list(psm):
        generated, consumed = obj_power_now(obj)
        income, loss = obj_score_now(obj)
        rows.append({
            'id': str(obj_id(obj)),
            'type': obj_type(obj),
            'contract': obj_contract(obj),
            'address': obj_address_key(obj),
            'path': json.dumps(obj_path(obj), ensure_ascii=False),
            'generated': generated,
            'consumed': consumed,
            'income': income,
            'loss': loss,
            'charge_now': obj_charge_now(obj),
            'wind_rotation': obj_wind_rotation_now(obj),
            'failed': obj_failed(obj),
        })
    return rows


def extract_network_rows(psm: Any) -> List[Dict[str, Any]]:
    rows = []
    for idx, net in get_network_items(psm):
        rows.append({
            'network_index': idx,
            'location': json.dumps(net_location(net), ensure_ascii=False),
            'upflow': net_upflow(net),
            'downflow': net_downflow(net),
            'losses': net_losses(net),
        })
    return rows


def aggregate_objects(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        'gen_total': 0.0,
        'cons_total': 0.0,
        'income_total': 0.0,
        'loss_total': 0.0,
        'by_type': {},
        'storages': [],
    }
    for row in rows:
        typ = row['type']
        bt = info['by_type'].setdefault(typ, {
            'count': 0,
            'generated': 0.0,
            'consumed': 0.0,
            'income': 0.0,
            'loss': 0.0,
        })
        bt['count'] += 1
        bt['generated'] += row['generated']
        bt['consumed'] += row['consumed']
        bt['income'] += row['income']
        bt['loss'] += row['loss']

        info['gen_total'] += row['generated']
        info['cons_total'] += row['consumed']
        info['income_total'] += row['income']
        info['loss_total'] += row['loss']

        if row['type'] == 'storage':
            info['storages'].append({'id': row['address'].split('|')[0], 'soc': safe_float(row['charge_now'], 0.0)})
    return info


def aggregate_networks(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    return {
        'upflow_total': sum(r['upflow'] for r in rows),
        'downflow_total': sum(r['downflow'] for r in rows),
        'losses_total': sum(r['losses'] for r in rows),
    }


def count_forecast_objects(object_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {k: 0 for k in OBJECT_TYPE_TO_FORECAST}
    for row in object_rows:
        typ = row.get('type')
        if typ in counts:
            counts[typ] += 1
    return counts


def aggregate_forecast_load(bundle: Dict[str, Dict[str, Any]], object_rows: List[Dict[str, Any]], tick: int) -> float:
    counts = count_forecast_objects(object_rows)
    total = 0.0
    for typ, fc_name in OBJECT_TYPE_TO_FORECAST.items():
        total += counts.get(typ, 0) * get_forecast_value(bundle, fc_name, tick)
    return total


def predict_total_losses(state: Dict[str, Any], total_gen: float, total_load: float) -> float:
    model = state.setdefault('loss_model', dict(LOSS_MODEL_PRIOR))
    gq = safe_float(model.get('gen_quad', LOSS_MODEL_PRIOR['gen_quad']), LOSS_MODEL_PRIOR['gen_quad'])
    gl = safe_float(model.get('gen_lin', LOSS_MODEL_PRIOR['gen_lin']), LOSS_MODEL_PRIOR['gen_lin'])
    lq = safe_float(model.get('load_quad', LOSS_MODEL_PRIOR['load_quad']), LOSS_MODEL_PRIOR['load_quad'])
    ll = safe_float(model.get('load_lin', LOSS_MODEL_PRIOR['load_lin']), LOSS_MODEL_PRIOR['load_lin'])
    base = safe_float(model.get('base', LOSS_MODEL_PRIOR['base']), LOSS_MODEL_PRIOR['base'])
    scale = safe_float(model.get('scale', 1.0), 1.0)
    pred = gq * total_gen * total_gen + gl * total_gen + lq * total_load * total_load + ll * total_load + base
    pred = max(0.0, pred)
    return pred * clamp(scale, 0.6, 1.6)


def get_forecast_value(bundle: Dict[str, Dict[str, Any]], name: str, tick: int) -> float:
    item = bundle.get(name)
    if not item:
        return 0.0
    data = item.get('data', [])
    if tick < 0 or tick >= len(data):
        return 0.0
    return safe_float(data[tick], 0.0)


def get_forecast_spread(bundle: Dict[str, Dict[str, Any]], name: str, fallback: float = 0.0) -> float:
    item = bundle.get(name)
    if not item:
        return fallback
    return safe_float(item.get('spread', fallback), fallback)


def update_forecast_error_log(tick: int, state: Dict[str, Any], object_rows: List[Dict[str, Any]], weather: Dict[str, float], bundle: Dict[str, Dict[str, Any]], total_consumed: float) -> None:
    actuals = {
        'hospital': 0.0,
        'factory': 0.0,
        'office': 0.0,
        'houseA': 0.0,
        'houseB': 0.0,
        'sun': weather['sun'],
        'wind': weather['wind'],
        'total_load': total_consumed,
    }
    for row in object_rows:
        typ = row['type']
        if typ in actuals:
            actuals[typ] += row['consumed']
    current_fc = {
        'hospital': get_forecast_value(bundle, 'hospital', tick),
        'factory': get_forecast_value(bundle, 'factory', tick),
        'office': get_forecast_value(bundle, 'office', tick),
        'houseA': get_forecast_value(bundle, 'houseA', tick),
        'houseB': get_forecast_value(bundle, 'houseB', tick),
        'sun': get_forecast_value(bundle, 'sun', tick),
        'wind': get_forecast_value(bundle, 'wind', tick),
        'total_load': aggregate_forecast_load(bundle, object_rows, tick),
    }
    for name, fc in current_fc.items():
        actual = actuals.get(name, 0.0)
        append_csv(FORECAST_ERRORS_CSV, ['tick', 'metric', 'forecast', 'actual', 'error', 'abs_error'], {
            'tick': tick,
            'metric': name,
            'forecast': round(fc, 6),
            'actual': round(actual, 6),
            'error': round(actual - fc, 6),
            'abs_error': round(abs(actual - fc), 6),
        })


def update_models(state: Dict[str, Any], object_rows: List[Dict[str, Any]], weather: Dict[str, float], bundle: Dict[str, Dict[str, Any]], tick: int, cfg: Optional[Dict[str, Any]] = None, total_consumed: Optional[float] = None, total_losses: Optional[float] = None) -> None:
    sun_now = max(0.0, weather['sun'])
    wind_now = max(0.0, weather['wind'])
    hist = state.setdefault('weather_history', {'wind': [], 'sun': []})
    hist['wind'] = (hist.get('wind') or [])[-12:] + [wind_now]
    hist['sun'] = (hist.get('sun') or [])[-12:] + [sun_now]

    total_fc_now = aggregate_forecast_load(bundle, object_rows, tick)
    if total_consumed is not None and total_fc_now > 1e-6:
        load_bias = total_consumed / max(total_fc_now, 1e-6)
        load_bias = clamp(load_bias, 0.28, 1.10)
        state['load_bias_total'] = 0.90 * safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) + 0.10 * load_bias
        pred_total = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) * total_fc_now
        state['load_abs_err'] = 0.90 * safe_float(state.get('load_abs_err', 2.0), 2.0) + 0.10 * abs(total_consumed - pred_total)

    if total_consumed is not None and total_losses is not None:
        pred_loss = predict_total_losses(state, sum(r['generated'] for r in object_rows), total_consumed)
        if pred_loss > 1e-6:
            scale = total_losses / pred_loss
            lm = state.setdefault('loss_model', dict(LOSS_MODEL_PRIOR))
            lm['scale'] = 0.88 * safe_float(lm.get('scale', 1.0), 1.0) + 0.12 * clamp(scale, 0.55, 1.8)

    for row in object_rows:
        key = row['address']
        typ = row['type']
        if typ == 'solar':
            model = get_model(state, key, 'solar')
            actual = row['generated']
            if sun_now > 0.05 and actual >= 0.0:
                est = actual / max(sun_now, 1e-6)
                model['factor'] = 0.90 * safe_float(model.get('factor', 0.65), 0.65) + 0.10 * clamp(est, 0.0, 1.6)
            pred = safe_float(model.get('factor', 0.65), 0.65) * sun_now
            model['err'] = 0.88 * safe_float(model.get('err', 0.8), 0.8) + 0.12 * abs(actual - pred)
        elif typ == 'wind':
            model = get_model(state, key, 'wind')
            actual = row['generated']
            rotation_now = max(0.0, safe_float(row.get('wind_rotation', 0.0), 0.0))
            failed_now = safe_int(row.get('failed', 0), 0)
            model['max_power_seen'] = max(safe_float(model.get('max_power_seen', 0.0), 0.0), actual)
            if wind_now > 0.2 and actual >= 0.0:
                est = actual / max(wind_now ** 3, 1e-6)
                model['factor'] = 0.94 * safe_float(model.get('factor', 0.0048), 0.0048) + 0.06 * clamp(est, 0.0, 0.02)
            if wind_now > 0.2 and rotation_now > 0.03:
                ratio = rotation_now / max(wind_now, 1e-6)
                model['wind_to_rot'] = 0.94 * safe_float(model.get('wind_to_rot', 0.040), 0.040) + 0.06 * clamp(ratio, 0.012, 0.090)
            if rotation_now > 0.05 and actual >= 0.0:
                est_rot = actual / max(rotation_now ** 3, 1e-6)
                model['rot_factor'] = 0.92 * safe_float(model.get('rot_factor', 80.0), 80.0) + 0.08 * clamp(est_rot, 12.0, 180.0)
                update_wind_rot_curve(model, rotation_now, actual)
            pred_direct = safe_float(model.get('factor', 0.0048), 0.0048) * (wind_now ** 3)
            pred_rot = safe_float(model.get('rot_factor', 80.0), 80.0) * (rotation_now ** 3)
            pred_curve = estimate_wind_from_curve(model, rotation_now)
            pred = 0.55 * pred_direct + 0.45 * pred_rot
            if pred_curve is not None:
                pred = 0.30 * pred_direct + 0.25 * pred_rot + 0.45 * pred_curve
            if failed_now > 0:
                pred *= 0.70
            elif safe_int(model.get('last_failed', 0), 0) > 0:
                pred *= 0.86
            model['err'] = 0.90 * safe_float(model.get('err', 2.5), 2.5) + 0.10 * abs(actual - pred)
            model['last_failed'] = failed_now
        elif typ in OBJECT_TYPE_TO_FORECAST:
            model = get_model(state, key, 'load')
            actual = row['consumed']
            fc_name = OBJECT_TYPE_TO_FORECAST.get(typ)
            fc_now = get_forecast_value(bundle, fc_name, tick)
            type_prior = safe_float(LOAD_TYPE_BIAS_PRIORS.get(typ, LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
            base_bias = 0.55 * safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) + 0.45 * type_prior
            if fc_now > 0.05 and actual >= 0.0:
                est_bias = actual / max(fc_now, 1e-6)
                model['bias'] = 0.92 * safe_float(model.get('bias', base_bias), base_bias) + 0.08 * clamp(est_bias, 0.20, 1.20)
            else:
                model['bias'] = 0.97 * safe_float(model.get('bias', base_bias), base_bias) + 0.03 * base_bias
            pred = clamp(safe_float(model.get('bias', base_bias), base_bias), 0.20, 1.20) * max(fc_now, 0.0)
            model['err'] = 0.92 * safe_float(model.get('err', 0.6), 0.6) + 0.08 * abs(actual - pred)


def analyze_exchange(exchange_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def weighted_avg(num: float, den: float) -> Optional[float]:
        return None if den <= 1e-9 else num / den

    stats = {
        'buy': {'asked': 0.0, 'contracted': 0.0, 'instant': 0.0, 'weighted_asked': 0.0, 'weighted_contracted': 0.0},
        'sell': {'asked': 0.0, 'contracted': 0.0, 'instant': 0.0, 'weighted_asked': 0.0, 'weighted_contracted': 0.0},
        'flat': {'asked': 0.0, 'contracted': 0.0, 'instant': 0.0, 'weighted_asked': 0.0, 'weighted_contracted': 0.0},
    }
    for row in exchange_rows:
        side = row.get('side', 'flat')
        bucket = stats.get(side, stats['flat'])
        asked = abs(row['askedAmount'])
        contracted = abs(row['contractedAmount'])
        instant = abs(row['instantAmount'])
        asked_price = row['askedPrice']
        contracted_price = row['contractedPrice']
        bucket['asked'] += asked
        bucket['contracted'] += contracted
        bucket['instant'] += instant
        bucket['weighted_asked'] += asked * asked_price
        bucket['weighted_contracted'] += contracted * contracted_price
    sell_avg_contracted = weighted_avg(stats['sell']['weighted_contracted'], stats['sell']['contracted'])
    sell_fill = None
    if stats['sell']['asked'] > 1e-9:
        sell_fill = stats['sell']['contracted'] / stats['sell']['asked']
    return {
        'buy_asked': stats['buy']['asked'],
        'buy_contracted': stats['buy']['contracted'],
        'buy_instant': stats['buy']['instant'],
        'buy_avg_asked_price': weighted_avg(stats['buy']['weighted_asked'], stats['buy']['asked']),
        'buy_avg_contracted_price': weighted_avg(stats['buy']['weighted_contracted'], stats['buy']['contracted']),
        'sell_asked': stats['sell']['asked'],
        'sell_contracted': stats['sell']['contracted'],
        'sell_instant': stats['sell']['instant'],
        'sell_avg_asked_price': weighted_avg(stats['sell']['weighted_asked'], stats['sell']['asked']),
        'sell_avg_contracted_price': sell_avg_contracted,
        'sell_fill_ratio': sell_fill,
        'instant_abs_total': stats['buy']['instant'] + stats['sell']['instant'] + stats['flat']['instant'],
    }


def compute_useful_energy(total_generated: float, total_consumed: float, total_losses: float) -> float:
    return total_generated - total_losses


def compute_balance_energy(total_generated: float, total_consumed: float, total_losses: float) -> float:
    return total_generated - total_consumed - total_losses


def compute_offer_cap(state: Dict[str, Any], cfg: Dict[str, Any], tick: int, useful_supply_now: float) -> float:
    prev_useful = state.get('prev_useful_supply_est')
    if prev_useful is None:
        if tick == 0 and STRICT_FIRST_TICK_CAP:
            return safe_float(cfg['exchangeAmountBuffer'], 10.0)
        return max(safe_float(cfg['exchangeAmountBuffer'], 10.0), useful_supply_now)
    return max(0.0, safe_float(prev_useful, 0.0)) * safe_float(cfg['exchangeAmountScaler'], 1.2) + safe_float(cfg['exchangeAmountBuffer'], 10.0)


def predict_object_generation(state: Dict[str, Any], row: Dict[str, Any], fc_sun: float, fc_wind: float, sun_spread: float, wind_spread: float, cfg: Optional[Dict[str, Any]] = None, weather: Optional[Dict[str, Any]] = None, step_ahead: int = 1) -> float:
    key = row['address']
    typ = row['type']
    cfg = cfg or {}
    weather = weather or {}
    delay = max(0, safe_int(cfg.get('weatherEffectsDelay', 0), 0))
    blend = 1.0 if step_ahead > delay else (step_ahead / max(1.0, float(delay + 1)))
    if typ == 'solar':
        model = get_model(state, key, 'solar')
        current_sun = max(0.0, safe_float(weather.get('sun', 0.0), 0.0))
        eff_sun = max(0.0, (1.0 - blend) * current_sun + blend * max(0.0, fc_sun - 0.20 * sun_spread))
        pred = safe_float(model.get('factor', 0.65), 0.65) * eff_sun
        pred *= clamp(1.0 - 0.03 * safe_float(model.get('err', 0.8), 0.8), 0.82, 1.00)
        return clamp(pred, 0.0, safe_float(cfg.get('maxSolarPower', 20.0), 20.0))
    if typ == 'wind':
        model = get_model(state, key, 'wind')
        current_wind = max(0.0, safe_float(weather.get('wind', 0.0), 0.0))
        current_rot = max(0.0, safe_float(row.get('wind_rotation', 0.0), 0.0))
        max_wind_power = safe_float(cfg.get('maxWindPower', 20.0), 20.0)
        safe_wind = max(0.0, fc_wind - 0.35 * wind_spread)
        eff_wind = max(0.0, (1.0 - blend) * current_wind + blend * safe_wind)
        rot_ratio = safe_float(model.get('wind_to_rot', 0.040), 0.040)
        inertia = clamp(1.0 - 1.0 / max(2.0, float(delay + step_ahead + 1)), 0.45, 0.88)
        projected_rot = max(0.0, inertia * current_rot + (1.0 - inertia) * rot_ratio * eff_wind)
        direct = safe_float(model.get('factor', 0.0048), 0.0048) * (eff_wind ** 3)
        rot_based = safe_float(model.get('rot_factor', 80.0), 80.0) * (projected_rot ** 3)
        curve_based = estimate_wind_from_curve(model, projected_rot)
        pred = 0.62 * direct + 0.38 * rot_based
        if curve_based is not None:
            pred = 0.28 * direct + 0.24 * rot_based + 0.48 * curve_based
        max_seen = safe_float(model.get('max_power_seen', 0.0), 0.0)
        if max_seen > 0.0:
            pred = min(pred, min(max_wind_power, 1.10 * max_seen + 0.4))
        pred *= clamp(1.0 - 0.05 * safe_float(model.get('err', 2.5), 2.5), 0.70, 1.00)
        failed_now = safe_int(row.get('failed', 0), 0)
        if failed_now > 0:
            pred *= 0.60
        elif safe_int(model.get('last_failed', 0), 0) > 0:
            pred *= 0.82
        if eff_wind > safe_float(cfg.get('weatherMaxWind', 15.0), 15.0) * 0.85:
            pred *= 0.90
        return clamp(pred, 0.0, max_wind_power)
    return 0.0


def predict_object_load(state: Dict[str, Any], row: Dict[str, Any], forecast_value: float) -> float:
    key = row['address']
    typ = row['type']
    if typ not in OBJECT_TYPE_TO_FORECAST:
        return 0.0
    model = get_model(state, key, 'load')
    type_prior = safe_float(LOAD_TYPE_BIAS_PRIORS.get(typ, LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
    base_bias = 0.55 * safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) + 0.45 * type_prior
    model_bias = safe_float(model.get('bias', base_bias), base_bias)
    bias = 0.60 * model_bias + 0.40 * base_bias
    return max(0.0, forecast_value * clamp(bias, 0.20, 1.20))


def forecast_window(state: Dict[str, Any], object_rows: List[Dict[str, Any]], bundle: Dict[str, Dict[str, Any]], tick: int, game_length: int, horizon: int) -> List[Dict[str, Any]]:
    runtime_cfg = state.get('cfg_runtime', {})
    wind_spread = max(get_forecast_spread(bundle, 'wind', 0.0), safe_float(runtime_cfg.get('corridorWind', 0.5), 0.5))
    sun_spread = max(get_forecast_spread(bundle, 'sun', 0.0), safe_float(runtime_cfg.get('corridorSun', 0.5), 0.5))
    weather = state.get('weather_runtime', {})
    out: List[Dict[str, Any]] = []
    last_tick = max(0, game_length - 1)
    start_tick = min(last_tick, tick + 1)
    end_tick = min(last_tick, tick + horizon)
    for t in range(start_tick, end_tick + 1):
        step_ahead = max(1, t - tick)
        fc_sun = get_forecast_value(bundle, 'sun', t)
        fc_wind = get_forecast_value(bundle, 'wind', t)
        total_gen = 0.0
        total_load = 0.0
        type_totals: Dict[str, Dict[str, float]] = {}
        for row in object_rows:
            gen_pred = predict_object_generation(state, row, fc_sun, fc_wind, sun_spread, wind_spread, cfg=runtime_cfg, weather=weather, step_ahead=step_ahead)
            total_gen += gen_pred
            type_totals.setdefault(row['type'], {'gen': 0.0, 'load': 0.0})
            type_totals[row['type']]['gen'] += gen_pred
        for row in object_rows:
            typ = row['type']
            fc_name = OBJECT_TYPE_TO_FORECAST.get(typ)
            if not fc_name:
                continue
            load_pred = predict_object_load(state, row, get_forecast_value(bundle, fc_name, t))
            total_load += load_pred
            type_totals.setdefault(typ, {'gen': 0.0, 'load': 0.0})
            type_totals[typ]['load'] += load_pred
        loss_pred = predict_total_losses(state, total_gen, total_load)
        useful_supply_pred = max(0.0, total_gen - loss_pred)
        balance_pred = total_gen - total_load - loss_pred
        out.append({
            'tick': t,
            'sun': fc_sun,
            'wind': fc_wind,
            'total_gen_pred': total_gen,
            'total_load_pred': total_load,
            'total_loss_pred': loss_pred,
            'balance_pred': balance_pred,
            'useful_supply_pred': useful_supply_pred,
            'type_totals': type_totals,
        })
    return out


def update_cumulative_state(state: Dict[str, Any], obj_agg: Dict[str, Any], object_rows: List[Dict[str, Any]], total_generated: float, total_consumed: float, total_losses: float, useful_now: float, market_stats: Dict[str, Any]) -> Dict[str, Any]:
    cumulative = state.setdefault('cumulative', {})
    cumulative['solar_generated'] = safe_float(cumulative.get('solar_generated', 0.0), 0.0) + safe_float(obj_agg['by_type'].get('solar', {}).get('generated', 0.0), 0.0)
    cumulative['wind_generated'] = safe_float(cumulative.get('wind_generated', 0.0), 0.0) + safe_float(obj_agg['by_type'].get('wind', {}).get('generated', 0.0), 0.0)
    cumulative['gross_generated'] = safe_float(cumulative.get('gross_generated', 0.0), 0.0) + total_generated
    cumulative['gross_consumed'] = safe_float(cumulative.get('gross_consumed', 0.0), 0.0) + total_consumed
    cumulative['losses'] = safe_float(cumulative.get('losses', 0.0), 0.0) + total_losses
    cumulative['useful_energy'] = safe_float(cumulative.get('useful_energy', 0.0), 0.0) + useful_now
    cumulative['buy_asked'] = safe_float(cumulative.get('buy_asked', 0.0), 0.0) + safe_float(market_stats.get('buy_asked', 0.0), 0.0)
    cumulative['buy_contracted'] = safe_float(cumulative.get('buy_contracted', 0.0), 0.0) + safe_float(market_stats.get('buy_contracted', 0.0), 0.0)
    cumulative['sell_asked'] = safe_float(cumulative.get('sell_asked', 0.0), 0.0) + safe_float(market_stats.get('sell_asked', 0.0), 0.0)
    cumulative['sell_contracted'] = safe_float(cumulative.get('sell_contracted', 0.0), 0.0) + safe_float(market_stats.get('sell_contracted', 0.0), 0.0)
    cumulative['instant_abs'] = safe_float(cumulative.get('instant_abs', 0.0), 0.0) + safe_float(market_stats.get('instant_abs_total', 0.0), 0.0)
    obj_cum = state.setdefault('object_cum', {})
    for row in object_rows:
        key = row['address']
        entry = obj_cum.setdefault(key, {'generated': 0.0, 'consumed': 0.0, 'income': 0.0, 'loss': 0.0, 'ticks': 0})
        entry['generated'] += row['generated']
        entry['consumed'] += row['consumed']
        entry['income'] += row['income']
        entry['loss'] += row['loss']
        entry['ticks'] += 1
    return cumulative



def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(float(v) for v in values)
    if len(vals) == 1:
        return vals[0]
    q = clamp(float(q), 0.0, 1.0)
    pos = q * (len(vals) - 1)
    lo = int(pos)
    hi = min(len(vals) - 1, lo + 1)
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def contiguous_windows(flags: List[bool], min_len: int = 1) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    start: Optional[int] = None
    for i, flag in enumerate(flags):
        if flag and start is None:
            start = i
        elif not flag and start is not None:
            if i - start >= min_len:
                out.append((start, i - 1))
            start = None
    if start is not None and len(flags) - start >= min_len:
        out.append((start, len(flags) - 1))
    return out


def build_forecast_profile(bundle: Dict[str, Dict[str, Any]], object_rows: List[Dict[str, Any]], game_length: int) -> Dict[str, Any]:
    rows = min(game_length, safe_int(bundle.get('_meta', {}).get('rows', game_length), game_length))
    if rows <= 0:
        return {'rows': 0, 'ticks': [], 'windows': {}}
    sun = [get_forecast_value(bundle, 'sun', t) for t in range(rows)]
    wind = [get_forecast_value(bundle, 'wind', t) for t in range(rows)]
    load = [aggregate_forecast_load(bundle, object_rows, t) for t in range(rows)]

    def _stats(vals: List[float]) -> Tuple[float, float]:
        if not vals:
            return 0.0, 1.0
        mean = sum(vals) / max(1, len(vals))
        var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals))
        return mean, max(var ** 0.5, 1e-6)

    sun_mean, sun_std = _stats(sun)
    wind_mean, wind_std = _stats(wind)
    load_mean, load_std = _stats(load)
    sun_q55 = percentile(sun, 0.55)
    sun_q70 = percentile(sun, 0.70)
    wind_q30 = percentile(wind, 0.30)
    wind_q70 = percentile(wind, 0.70)
    load_q70 = percentile(load, 0.70)
    load_q85 = percentile(load, 0.85)

    ticks: List[Dict[str, Any]] = []
    for t in range(rows):
        z_sun = (sun[t] - sun_mean) / sun_std
        z_wind = (wind[t] - wind_mean) / wind_std
        z_load = (load[t] - load_mean) / load_std
        solar_active = sun[t] >= max(1.0, sun_q55)
        solar_peak = sun[t] >= max(2.0, sun_q70)
        load_peak = load[t] >= load_q70
        load_extreme = load[t] >= load_q85
        wind_peak = wind[t] >= wind_q70
        wind_low = wind[t] <= wind_q30
        mixed_peak = solar_peak and load_peak
        combo_score = 0.52 * z_sun + 0.33 * z_load + 0.15 * z_wind
        risk_score = 0.50 * max(0.0, -z_sun) + 0.18 * max(0.0, -z_wind) + 0.42 * max(0.0, z_load)
        ticks.append({
            'tick': t,
            'sun': sun[t],
            'wind': wind[t],
            'load': load[t],
            'solar_active': solar_active,
            'solar_peak': solar_peak,
            'load_peak': load_peak,
            'load_extreme': load_extreme,
            'wind_peak': wind_peak,
            'wind_low': wind_low,
            'mixed_peak': mixed_peak,
            'combo_score': combo_score,
            'risk_score': risk_score,
            'tail_low_load': (t >= rows - 8 and load[t] <= load_mean),
            'charge_bias': 1.0 if mixed_peak or combo_score >= 0.80 else 0.0,
            'protect_bias': 1.0 if risk_score >= 0.80 or (wind_low and load_peak and not solar_active) else 0.0,
        })

    windows = {
        'solar_active': contiguous_windows([x['solar_active'] for x in ticks], min_len=3),
        'mixed_peak': contiguous_windows([x['mixed_peak'] for x in ticks], min_len=2),
        'wind_peak': contiguous_windows([x['wind_peak'] for x in ticks], min_len=2),
        'risk_peak': contiguous_windows([x['protect_bias'] > 0.5 for x in ticks], min_len=2),
    }
    return {
        'rows': rows,
        'ticks': ticks,
        'thresholds': {
            'sun_q55': sun_q55,
            'sun_q70': sun_q70,
            'wind_q30': wind_q30,
            'wind_q70': wind_q70,
            'load_q70': load_q70,
            'load_q85': load_q85,
        },
        'windows': windows,
    }


def get_or_build_forecast_profile(state: Dict[str, Any], bundle: Dict[str, Dict[str, Any]], object_rows: List[Dict[str, Any]], game_length: int) -> Dict[str, Any]:
    meta = bundle.get('_meta', {})
    key = (meta.get('source'), meta.get('path'), meta.get('rows'), tuple(sorted((r.get('address'), r.get('type')) for r in object_rows)))
    profile = state.get('forecast_profile')
    if isinstance(profile, dict) and profile.get('key') == key:
        return profile.get('data', {'rows': 0, 'ticks': [], 'windows': {}})
    data = build_forecast_profile(bundle, object_rows, game_length)
    state['forecast_profile'] = {'key': key, 'data': data}
    return data


def forecast_profile_context(profile: Dict[str, Any], tick: int, horizon: int = 12) -> Dict[str, Any]:
    ticks = profile.get('ticks', []) if isinstance(profile, dict) else []
    if not ticks:
        return {'current': {}, 'avg_combo_6': 0.0, 'avg_combo_12': 0.0, 'avg_risk_6': 0.0, 'avg_risk_12': 0.0, 'next_mixed_in': None, 'next_risk_in': None}
    idx = int(clamp(tick, 0, len(ticks) - 1))
    cur = ticks[idx]

    def _avg(name: str, n: int) -> float:
        end = min(len(ticks), idx + n + 1)
        vals = [safe_float(t.get(name, 0.0), 0.0) for t in ticks[idx:end]]
        return sum(vals) / max(1, len(vals))

    next_mixed_in = next((i - idx for i in range(idx, len(ticks)) if ticks[i].get('mixed_peak')), None)
    next_risk_in = next((i - idx for i in range(idx, len(ticks)) if ticks[i].get('protect_bias', 0.0) > 0.5), None)
    next_solar_in = next((i - idx for i in range(idx, len(ticks)) if ticks[i].get('solar_active')), None)
    return {
        'current': cur,
        'avg_combo_6': _avg('combo_score', 6),
        'avg_combo_12': _avg('combo_score', max(12, horizon)),
        'avg_risk_6': _avg('risk_score', 6),
        'avg_risk_12': _avg('risk_score', max(12, horizon)),
        'next_mixed_in': next_mixed_in,
        'next_risk_in': next_risk_in,
        'next_solar_in': next_solar_in,
    }


def compute_target_soc(cfg: Dict[str, Any], total_capacity: float, total_soc: float, future: List[Dict[str, Any]], fill_ratio: float, tick: int, game_length: int, loss_ratio: float = 0.10, profile_ctx: Optional[Dict[str, Any]] = None) -> float:
    base_ceil = total_capacity * SOC_CEIL_FRAC
    weighted_gap = 0.0
    weighted_surplus = 0.0
    raw_gap_sum = 0.0
    raw_surplus_sum = 0.0
    useful_gap_sum = 0.0
    max_gap = 0.0
    for i, row in enumerate(future):
        w = 1.0 / (i + 1)
        bal = safe_float(row.get('balance_pred', 0.0), 0.0)
        useful = safe_float(row.get('useful_supply_pred', 0.0), 0.0)
        gap = max(0.0, -bal)
        surplus = max(0.0, bal)
        weighted_gap += w * gap
        weighted_surplus += w * surplus
        raw_gap_sum += gap
        raw_surplus_sum += surplus
        max_gap = max(max_gap, gap)
        useful_gap_sum += w * max(0.0, 1.5 - useful)
    chronic_deficit = raw_gap_sum > max(4.0, 1.5 * raw_surplus_sum)
    floor = total_capacity * (0.06 if chronic_deficit else SOC_FLOOR_FRAC)
    target = floor + 0.72 * weighted_gap + 0.18 * useful_gap_sum - 0.08 * weighted_surplus
    target += 0.02 * total_capacity
    if fill_ratio < 0.60 and raw_surplus_sum > 0.0:
        target += 0.05 * total_capacity
    elif fill_ratio > 0.90 and raw_surplus_sum > raw_gap_sum:
        target -= 0.03 * total_capacity
    if profile_ctx:
        current = profile_ctx.get('current', {})
        avg_combo = safe_float(profile_ctx.get('avg_combo_12', 0.0), 0.0)
        avg_risk = safe_float(profile_ctx.get('avg_risk_12', 0.0), 0.0)
        next_risk_in = profile_ctx.get('next_risk_in')
        next_mixed_in = profile_ctx.get('next_mixed_in')
        if current.get('mixed_peak') or current.get('solar_active'):
            target = max(target, 0.36 * total_capacity)
        if next_risk_in is not None and 0 <= next_risk_in <= 20:
            risk_floor = (0.62 - 0.014 * min(next_risk_in, 20)) * total_capacity
            target = max(target, risk_floor)
        if avg_risk > avg_combo + 0.12:
            target = max(target, 0.42 * total_capacity)
        if next_mixed_in is not None and 0 <= next_mixed_in <= 10 and next_risk_in is not None and next_risk_in <= 20:
            target = max(target, 0.50 * total_capacity)
        if current.get('tail_low_load'):
            target -= 0.04 * total_capacity
    target += 0.12 * max_gap
    ticks_left = max(0, game_length - tick)
    if ticks_left <= ENDGAME_TICKS:
        floor = 0.0
        target = max(0.0, 0.10 * weighted_gap)
    return clamp(target, floor, base_ceil)


def storage_policy(cfg: Dict[str, Any], storages: List[Dict[str, Any]], balance_now: float, useful_now: float, future: List[Dict[str, Any]], fill_ratio: float, tick: int, game_length: int, loss_ratio: float = 0.10, profile_ctx: Optional[Dict[str, Any]] = None) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]], Dict[str, Any]]:
    if not storages:
        return [], [], {
            'target_soc': 0.0,
            'prep_soc': 0.0,
            'total_soc': 0.0,
            'charge_total': 0.0,
            'discharge_total': 0.0,
            'discharge_for_market': 0.0,
            'chronic_deficit': False,
            'floor_soc': 0.0,
        }
    cell_capacity = safe_float(cfg['cellCapacity'], 120.0)
    charge_rate = safe_float(cfg['cellChargeRate'], 15.0)
    discharge_rate = safe_float(cfg['cellDischargeRate'], 20.0)
    total_capacity = len(storages) * cell_capacity
    total_soc = sum(s['soc'] for s in storages)
    total_charge_rate = len(storages) * charge_rate
    total_discharge_rate = len(storages) * discharge_rate
    target_soc = compute_target_soc(cfg, total_capacity, total_soc, future, fill_ratio, tick, game_length, loss_ratio=loss_ratio, profile_ctx=profile_ctx)
    deficit_sum = sum(max(0.0, -safe_float(r.get('balance_pred', 0.0), 0.0)) for r in future)
    surplus_sum = sum(max(0.0, safe_float(r.get('balance_pred', 0.0), 0.0)) for r in future)
    next_deficit = max(0.0, -safe_float(future[0].get('balance_pred', 0.0), 0.0)) if future else 0.0
    next_surplus = max(0.0, safe_float(future[0].get('balance_pred', 0.0), 0.0)) if future else 0.0
    chronic_deficit = deficit_sum > max(4.0, 1.5 * surplus_sum)
    floor_frac = 0.06 if chronic_deficit else SOC_FLOOR_FRAC
    if tick >= game_length - ENDGAME_TICKS:
        floor_frac = 0.0
    current_profile = profile_ctx.get('current', {}) if profile_ctx else {}
    next_risk_in = profile_ctx.get('next_risk_in') if profile_ctx else None
    next_mixed_in = profile_ctx.get('next_mixed_in') if profile_ctx else None
    if next_risk_in is not None and 0 <= next_risk_in <= 18:
        floor_frac = max(floor_frac, 0.10)
    floor_soc = total_capacity * floor_frac
    prep_soc = target_soc
    if next_risk_in is not None and 0 <= next_risk_in <= 18:
        prep_soc = max(prep_soc, (0.68 - 0.016 * min(next_risk_in, 18)) * total_capacity)
    elif current_profile.get('mixed_peak') or current_profile.get('solar_active'):
        prep_soc = max(prep_soc, 0.42 * total_capacity)
    if next_mixed_in is not None and 0 <= next_mixed_in <= 8 and next_risk_in is not None and 0 <= next_risk_in <= 18:
        prep_soc = max(prep_soc, 0.58 * total_capacity)
    prep_soc = clamp(prep_soc, floor_soc, total_capacity * SOC_CEIL_FRAC)
    protected_soc = max(floor_soc, 0.80 * prep_soc if chronic_deficit else 0.72 * prep_soc)
    charge_total = 0.0
    discharge_total = 0.0
    discharge_for_market = 0.0

    current_deficit = max(0.0, -balance_now)
    current_surplus = max(0.0, balance_now)
    discharge_budget = max(0.0, total_soc - protected_soc)
    charge_room = max(0.0, total_capacity * SOC_CEIL_FRAC - total_soc)

    if current_deficit > 0.0 and discharge_budget > 0.0:
        desired = current_deficit + 0.22 * next_deficit
        if chronic_deficit or current_profile.get('protect_bias', 0.0) > 0.5:
            desired += 0.12 * deficit_sum + 0.22 * next_deficit
        discharge_total = min(total_discharge_rate, discharge_budget, desired)

    should_charge = False
    charge_gain = 1.00
    if current_surplus > 0.0 and charge_room > 0.0:
        if total_soc < prep_soc:
            should_charge = True
            charge_gain = max(charge_gain, 1.08)
        if chronic_deficit or next_deficit > 1.0:
            should_charge = True
            charge_gain = max(charge_gain, 1.12)
        if current_profile.get('mixed_peak') or current_profile.get('charge_bias', 0.0) > 0.5:
            should_charge = True
            charge_gain = max(charge_gain, 1.18)
        if next_risk_in is not None and 0 <= next_risk_in <= 18:
            should_charge = True
            charge_gain = max(charge_gain, 1.32)
        if next_mixed_in is not None and 0 <= next_mixed_in <= 6 and next_risk_in is not None and 0 <= next_risk_in <= 18:
            should_charge = True
            charge_gain = max(charge_gain, 1.38)
        if should_charge:
            desired_charge = max(current_surplus * charge_gain, 0.40 * max(0.0, prep_soc - total_soc))
            charge_total = min(total_charge_rate, charge_room, desired_charge)
    elif current_surplus <= 0.0 and next_surplus > 3.0 and total_soc < prep_soc and charge_room > 0.0:
        charge_total = min(total_charge_rate, charge_room, 0.55 * next_surplus)

    if tick >= game_length - ENDGAME_TICKS and total_soc > floor_soc:
        extra = min(max(0.0, total_discharge_rate - discharge_total), max(0.0, total_soc - floor_soc))
        if balance_now >= -0.5 and useful_now > 0.0 and (next_risk_in is None or next_risk_in > 3):
            discharge_for_market = min(extra, max(0.0, total_soc - prep_soc))
            discharge_total += discharge_for_market

    if charge_total > 0.0 and discharge_total > 0.0:
        if charge_total >= discharge_total:
            discharge_total = 0.0
            discharge_for_market = 0.0
        else:
            charge_total = 0.0

    charge_orders: List[Tuple[str, float]] = []
    discharge_orders: List[Tuple[str, float]] = []
    rem = charge_total
    for s in sorted(storages, key=lambda x: x['soc']):
        if rem <= 1e-9:
            break
        room = max(0.0, cell_capacity - s['soc'])
        amt = min(rem, charge_rate, room)
        if amt >= 1e-9:
            charge_orders.append((s['id'], round_vol(amt)))
            rem -= amt
    rem = discharge_total
    floor_per_cell = floor_soc / max(1, len(storages))
    for s in sorted(storages, key=lambda x: -x['soc']):
        if rem <= 1e-9:
            break
        avail = max(0.0, s['soc'] - floor_per_cell)
        amt = min(rem, discharge_rate, avail)
        if amt >= 1e-9:
            discharge_orders.append((s['id'], round_vol(amt)))
            rem -= amt
    charge_total = sum(v for _, v in charge_orders)
    discharge_total = sum(v for _, v in discharge_orders)
    discharge_for_market = min(discharge_total, max(0.0, discharge_for_market))
    return charge_orders, discharge_orders, {
        'target_soc': target_soc,
        'prep_soc': prep_soc,
        'total_soc': total_soc,
        'charge_total': charge_total,
        'discharge_total': discharge_total,
        'discharge_for_market': discharge_for_market,
        'chronic_deficit': chronic_deficit,
        'floor_soc': floor_soc,
    }


def analyze_topology(object_rows: List[Dict[str, Any]], network_rows: List[Dict[str, Any]], total_generated: float) -> Dict[str, Any]:
    by_branch: Dict[str, Dict[str, float]] = {}
    warnings: List[str] = []
    branch_mix: Dict[str, Dict[str, int]] = {}
    for row in network_rows:
        try:
            location = json.loads(row.get('location', '[]')) if isinstance(row.get('location'), str) else row.get('location', [])
        except Exception:
            location = row.get('location', [])
        branch_key = 'unknown'
        if isinstance(location, list) and location:
            first = location[0]
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                branch_key = f'{first[0]}:{first[1]}'
        bucket = by_branch.setdefault(branch_key, {'losses': 0.0, 'upflow': 0.0, 'downflow': 0.0, 'count': 0})
        bucket['losses'] += safe_float(row.get('losses', 0.0), 0.0)
        bucket['upflow'] += safe_float(row.get('upflow', 0.0), 0.0)
        bucket['downflow'] += safe_float(row.get('downflow', 0.0), 0.0)
        bucket['count'] += 1
    for row in object_rows:
        try:
            path = json.loads(row.get('path', '[]')) if isinstance(row.get('path'), str) else row.get('path', [])
        except Exception:
            path = []
        branch_key = 'unknown'
        if isinstance(path, list) and path:
            first = path[0]
            if isinstance(first, (list, tuple)) and first and isinstance(first[0], (list, tuple)) and len(first[0]) >= 2:
                branch_key = f'{first[0][0]}:{first[0][1]}'
        mix = branch_mix.setdefault(branch_key, {'gen': 0, 'load': 0, 'hospital': 0, 'factory': 0})
        if row.get('type') in ('solar', 'wind'):
            mix['gen'] += 1
        if row.get('type') in ('houseA', 'houseB', 'office', 'factory', 'hospital'):
            mix['load'] += 1
        if row.get('type') == 'hospital':
            mix['hospital'] += 1
        if row.get('type') == 'factory':
            mix['factory'] += 1
    branch_losses_sorted = sorted(({'branch': k, **v} for k, v in by_branch.items()), key=lambda x: x['losses'], reverse=True)
    if total_generated > 1e-9:
        total_branch_losses = sum(x['losses'] for x in branch_losses_sorted)
        if total_branch_losses > 0.35 * total_generated:
            warnings.append('high_network_losses')
        if branch_losses_sorted and branch_losses_sorted[0]['losses'] > 0.55 * total_branch_losses:
            warnings.append('losses_concentrated_in_one_branch')
    for branch, mix in branch_mix.items():
        if mix['gen'] > 0 and mix['load'] > 0:
            warnings.append(f'mixed_branch:{branch}')
    hospital_count = sum(1 for r in object_rows if r.get('type') == 'hospital')
    if hospital_count > 0 and sum(safe_float(r.get('consumed', 0.0), 0.0) for r in object_rows if r.get('type') == 'hospital') <= 0.0:
        warnings.append('hospital_zero_consumption')
    return {'branch_losses': branch_losses_sorted[:10], 'branch_mix': branch_mix, 'warnings': warnings}


def compute_reserve(state: Dict[str, Any], future: List[Dict[str, Any]], object_rows: List[Dict[str, Any]], network_losses: float, useful_supply_now: float, profile_ctx: Optional[Dict[str, Any]] = None) -> float:
    gen_total = sum(r['generated'] for r in object_rows)
    wind_gen = sum(r['generated'] for r in object_rows if r['type'] == 'wind')
    wind_share = wind_gen / max(gen_total, 1e-9) if gen_total > 0 else 0.0
    abs_err = safe_float(state.get('abs_err_ewma', 1.2), 1.2)
    loss_ratio = safe_float(state.get('loss_ratio_ewma', 0.18), 0.18)
    next_useful = safe_float(future[0].get('useful_supply_pred', 0.0), 0.0) if future else useful_supply_now
    next_balance = safe_float(future[0].get('balance_pred', 0.0), 0.0) if future else 0.0
    reserve = 0.18 + 0.28 * abs_err + 0.08 * max(0.0, loss_ratio - 0.10) * max(gen_total, useful_supply_now)
    reserve += 0.06 * wind_share * max(wind_gen, next_useful)
    if next_useful < useful_supply_now:
        reserve += 0.14 * (useful_supply_now - next_useful)
    if next_balance < -2.0:
        reserve += 0.04 * abs(next_balance)
    if profile_ctx:
        current = profile_ctx.get('current', {})
        avg_combo = safe_float(profile_ctx.get('avg_combo_12', 0.0), 0.0)
        avg_risk = safe_float(profile_ctx.get('avg_risk_12', 0.0), 0.0)
        if current.get('mixed_peak') or avg_combo > avg_risk + 0.10:
            reserve *= 0.88
        if current.get('protect_bias', 0.0) > 0.5 or avg_risk > avg_combo + 0.10:
            reserve *= 1.10
    if useful_supply_now > 0.0:
        reserve = min(reserve, max(MIN_RESERVE, 0.18 * useful_supply_now))
    return max(MIN_RESERVE, reserve)


def build_ladder(sell_volume: float, market_ref: float, fill_ratio: float, max_tickets: int, cfg: Dict[str, Any], buy_ref: Optional[float] = None, profile_ctx: Optional[Dict[str, Any]] = None) -> List[Tuple[float, float]]:
    if sell_volume < MIN_ORDER_VOLUME:
        return []
    market_cap = safe_float(cfg.get('exchangeExternalBuy', 20.0), 20.0)
    gp_price = safe_float(cfg.get('exchangeExternalSell', 2.0), 2.0)
    step = safe_float(cfg.get('exchangeConsumerPriceStep', 0.2), 0.2)
    buy_ref = safe_float(buy_ref, market_ref if buy_ref is not None else market_ref)
    current = profile_ctx.get('current', {}) if profile_ctx else {}
    avg_combo = safe_float(profile_ctx.get('avg_combo_12', 0.0), 0.0) if profile_ctx else 0.0
    avg_risk = safe_float(profile_ctx.get('avg_risk_12', 0.0), 0.0) if profile_ctx else 0.0
    base_ref = max(market_ref, buy_ref)
    if fill_ratio < 0.55:
        base_ref -= 1.2 * step
    elif fill_ratio > 0.90:
        base_ref += 1.0 * step
    if current.get('mixed_peak') or avg_combo > avg_risk + 0.10:
        base_ref += 0.8 * step
    if current.get('protect_bias', 0.0) > 0.5 or avg_risk > avg_combo + 0.15:
        base_ref -= 0.6 * step
    base_ref = clamp(base_ref, gp_price + step, market_cap - step)

    if sell_volume < 1.5:
        prices = [max(gp_price + step, base_ref - step), min(base_ref + 2 * step, market_cap - step)]
        shares = [0.85, 0.15]
    elif fill_ratio < 0.55:
        prices = [max(gp_price + step, base_ref - 2 * step), base_ref, min(base_ref + 3 * step, market_cap - step)]
        shares = [0.82, 0.13, 0.05]
    elif fill_ratio > 0.92 and (current.get('mixed_peak') or avg_combo > avg_risk):
        prices = [max(gp_price + 2 * step, base_ref - step), min(base_ref + 2 * step, market_cap - step), min(base_ref + 5 * step, market_cap - step)]
        shares = [0.58, 0.27, 0.15]
    else:
        prices = [max(gp_price + step, base_ref - step), min(base_ref + 2 * step, market_cap - step), min(base_ref + 4 * step, market_cap - step)]
        shares = [0.70, 0.22, 0.08]
    prices = [round_price(p, price_max=market_cap, price_step=step) for p in prices]
    out: List[Tuple[float, float]] = []
    allocated = 0.0
    for i, share in enumerate(shares):
        if i == len(shares) - 1:
            vol = max(0.0, sell_volume - allocated)
        else:
            vol = round_vol(sell_volume * share)
            allocated += vol
        if vol >= MIN_ORDER_VOLUME:
            out.append((round_vol(vol), prices[i]))
    return out[:max_tickets]


def current_theoretical_metrics(state: Dict[str, Any], object_rows: List[Dict[str, Any]], weather: Dict[str, float], bundle: Dict[str, Dict[str, Any]], tick: int, cfg: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    sun_now = weather['sun']
    wind_now = weather['wind']
    cfg = cfg or state.get('cfg_runtime', {})
    sun_spread = max(get_forecast_spread(bundle, 'sun', 0.0), safe_float(cfg.get('corridorSun', 0.5), 0.5))
    wind_spread = max(get_forecast_spread(bundle, 'wind', 0.0), safe_float(cfg.get('corridorWind', 0.5), 0.5))
    object_prediction_rows = []
    totals = {
        'solar_theoretical_now': 0.0,
        'wind_theoretical_now': 0.0,
        'gross_theoretical_now': 0.0,
        'load_forecast_now': aggregate_forecast_load(bundle, object_rows, tick),
        'load_model_now': 0.0,
        'loss_theoretical_now': 0.0,
    }
    for row in object_rows:
        typ = row['type']
        gen_theoretical = predict_object_generation(state, row, sun_now, wind_now, sun_spread, wind_spread, cfg=cfg, weather=weather, step_ahead=0)
        if typ == 'solar':
            totals['solar_theoretical_now'] += gen_theoretical
        elif typ == 'wind':
            totals['wind_theoretical_now'] += gen_theoretical
        totals['gross_theoretical_now'] += gen_theoretical
        fc_name = OBJECT_TYPE_TO_FORECAST.get(typ)
        load_forecast = get_forecast_value(bundle, fc_name, tick) if fc_name else 0.0
        load_model = predict_object_load(state, row, load_forecast) if fc_name else 0.0
        totals['load_model_now'] += load_model
        object_prediction_rows.append({
            'tick': tick,
            'address': row['address'],
            'type': typ,
            'gen_actual': round(row['generated'], 6),
            'gen_theoretical_now': round(gen_theoretical, 6),
            'gen_model_error': round(row['generated'] - gen_theoretical, 6),
            'load_actual': round(row['consumed'], 6),
            'load_forecast_now': round(load_forecast, 6),
            'load_model_now': round(load_model, 6),
            'load_model_error': round(row['consumed'] - load_model, 6),
            'sun_now': round(sun_now, 6),
            'wind_now': round(wind_now, 6),
        })
    totals['loss_theoretical_now'] = predict_total_losses(state, totals['gross_theoretical_now'], totals['load_model_now'])
    return totals, object_prediction_rows


def normalize_cfg(raw: Dict[str, Any]) -> Dict[str, Any]:
    def g(name: str, default: float) -> float:
        return safe_float(raw.get(name, default), default)
    return {
        'exchangeMaxTickets': safe_int(raw.get('exchangeMaxTickets', 100), 100),
        'exchangeExternalSell': g('exchangeExternalSell', 2.0),
        'exchangeExternalBuy': g('exchangeExternalBuy', 20.0),
        'exchangeExternalInstantSell': g('exchangeExternalInstantSell', 1.5),
        'exchangeExternalInstantBuy': g('exchangeExternalInstantBuy', raw.get('exchangeExternalIntantBuy', 25.0)),
        'exchangeAmountScaler': g('exchangeAmountScaler', 1.2),
        'exchangeAmountBuffer': g('exchangeAmountBuffer', 10.0),
        'cellCapacity': g('cellCapacity', 120.0),
        'cellChargeRate': g('cellChargeRate', 15.0),
        'cellDischargeRate': g('cellDischargeRate', 20.0),
        'corridorSun': g('corridorSun', 0.5),
        'corridorWind': g('corridorWind', 0.5),
        'corridorFactory': g('corridorFactory', 0.5),
        'corridorOffice': g('corridorOffice', 0.5),
        'corridorHospital': g('corridorHospital', 0.25),
        'corridorHouseA': g('corridorHouseA', 0.5),
        'corridorHouseB': g('corridorHouseB', 0.5),
        'maxSolarPower': g('maxSolarPower', 20.0),
        'maxWindPower': g('maxWindPower', 20.0),
        'exchangeConsumerPriceStep': g('exchangeConsumerPriceStep', 0.2),
        'weatherEffectsDelay': safe_int(raw.get('weatherEffectsDelay', 0), 0),
        'weatherMaxWind': g('weatherMaxWind', 15.0),
    }


def log_tick_data(tick: int, state: Dict[str, Any], object_rows: List[Dict[str, Any]], network_rows: List[Dict[str, Any]], exchange_rows: List[Dict[str, Any]], summary_row: Dict[str, Any], strategy_row: Dict[str, Any], derived_row: Optional[Dict[str, Any]] = None, object_prediction_rows: Optional[List[Dict[str, Any]]] = None) -> None:
    ensure_dir(LOG_DIR)
    write_jsonl(TICK_SUMMARY_FILE, summary_row)
    write_jsonl(STRATEGY_DEBUG_FILE, strategy_row)
    obj_cum = state.get('object_cum', {})
    for row in object_rows:
        csv_row = {'tick': tick}
        csv_row.update(row)
        cum = obj_cum.get(row['address'], {})
        csv_row.update({
            'cum_generated': round(safe_float(cum.get('generated', 0.0), 0.0), 6),
            'cum_consumed': round(safe_float(cum.get('consumed', 0.0), 0.0), 6),
            'cum_income': round(safe_float(cum.get('income', 0.0), 0.0), 6),
            'cum_loss': round(safe_float(cum.get('loss', 0.0), 0.0), 6),
            'cum_ticks': safe_int(cum.get('ticks', 0), 0),
        })
        append_csv(OBJECTS_CSV, [
            'tick', 'id', 'type', 'contract', 'address', 'path',
            'generated', 'consumed', 'income', 'loss', 'charge_now', 'wind_rotation', 'failed',
            'cum_generated', 'cum_consumed', 'cum_income', 'cum_loss', 'cum_ticks',
        ], csv_row)
    for row in network_rows:
        csv_row = {'tick': tick}
        csv_row.update(row)
        append_csv(NETWORKS_CSV, ['tick', 'network_index', 'location', 'upflow', 'downflow', 'losses'], csv_row)
    for idx, row in enumerate(exchange_rows):
        csv_row = {'tick': tick, 'receipt_index': idx}
        csv_row.update(row)
        csv_row.update({
            'abs_askedAmount': round(abs(safe_float(row.get('askedAmount', 0.0), 0.0)), 6),
            'abs_contractedAmount': round(abs(safe_float(row.get('contractedAmount', 0.0), 0.0)), 6),
            'abs_instantAmount': round(abs(safe_float(row.get('instantAmount', 0.0), 0.0)), 6),
        })
        append_csv(EXCHANGE_CSV, [
            'tick', 'receipt_index', 'side', 'askedAmount', 'askedPrice', 'contractedAmount', 'contractedPrice', 'instantAmount',
            'abs_askedAmount', 'abs_contractedAmount', 'abs_instantAmount',
        ], csv_row)
    if derived_row is not None:
        append_csv(DERIVED_METRICS_CSV, list(derived_row.keys()), derived_row)
    if object_prediction_rows:
        for row in object_prediction_rows:
            append_csv(OBJECT_PREDICTIONS_CSV, list(row.keys()), row)


# =====================
# Main controller
# =====================

def controller(psm: Any) -> Dict[str, Any]:
    state = load_state()
    tick = get_tick(psm)
    if tick == 0:
        reset_runtime_outputs()
        state = default_state()
    game_length = get_game_length(psm)
    cfg = normalize_cfg(get_config_dict(psm))
    object_rows = extract_object_rows(psm)
    network_rows = extract_network_rows(psm)
    exchange_rows = [exchange_receipt_data(x) for x in get_exchange_list(psm)]

    total_generated, total_consumed, total_external, total_losses = get_total_power_tuple(psm)
    net_agg = aggregate_networks(network_rows)
    obj_agg = aggregate_objects(object_rows)
    topology = analyze_topology(object_rows, network_rows, total_generated)
    weather = {'wind': get_weather_now(psm, 'wind'), 'sun': get_weather_now(psm, 'sun')}
    forecast_bundle = get_forecast_bundle(psm)
    state['cfg_runtime'] = cfg
    state['weather_runtime'] = weather

    update_forecast_error_log(tick, state, object_rows, weather, forecast_bundle, total_consumed)
    update_models(state, object_rows, weather, forecast_bundle, tick, cfg, total_consumed=total_consumed, total_losses=total_losses)

    forecast_profile = get_or_build_forecast_profile(state, forecast_bundle, object_rows, game_length)
    profile_ctx = forecast_profile_context(forecast_profile, tick, horizon=max(12, LOOKAHEAD * 2))
    future = forecast_window(state, object_rows, forecast_bundle, tick, game_length, LOOKAHEAD)
    current_theoretical, object_prediction_rows = current_theoretical_metrics(state, object_rows, weather, forecast_bundle, tick, cfg)

    useful_raw = compute_useful_energy(total_generated, total_consumed, total_losses)
    useful_now = max(0.0, useful_raw)
    balance_now = compute_balance_energy(total_generated, total_consumed, total_losses)

    market_stats = analyze_exchange(exchange_rows)
    sell_avg_price = market_stats.get('sell_avg_contracted_price')
    buy_ref = market_stats.get('buy_avg_contracted_price') or market_stats.get('buy_avg_asked_price')
    fill_ratio_now = market_stats.get('sell_fill_ratio')
    exch_log = get_exchange_log(psm)
    if sell_avg_price is not None:
        state['market_ref'] = 0.76 * safe_float(state.get('market_ref', 7.2), 7.2) + 0.24 * sell_avg_price
    elif buy_ref is not None:
        state['market_ref'] = 0.85 * safe_float(state.get('market_ref', 7.2), 7.2) + 0.15 * safe_float(buy_ref, 7.2)
    elif tick > 0 and tick - 1 < len(exch_log):
        last_log_price = safe_float(exch_log[tick - 1], state.get('market_ref', 7.2))
        state['market_ref'] = 0.88 * safe_float(state.get('market_ref', 7.2), 7.2) + 0.12 * last_log_price
    if fill_ratio_now is not None:
        state['fill_ratio_ewma'] = 0.76 * safe_float(state.get('fill_ratio_ewma', 0.74), 0.74) + 0.24 * fill_ratio_now

    prev_useful_est = state.get('prev_useful_supply_est')
    if prev_useful_est is not None:
        err = useful_now - safe_float(prev_useful_est, 0.0)
        state['abs_err_ewma'] = 0.84 * safe_float(state.get('abs_err_ewma', 1.2), 1.2) + 0.16 * abs(err)
    if total_generated > 1e-9:
        current_loss_ratio = clamp(total_losses / total_generated, 0.0, 0.8)
        state['loss_ratio_ewma'] = 0.88 * safe_float(state.get('loss_ratio_ewma', 0.18), 0.18) + 0.12 * current_loss_ratio

    charge_orders, discharge_orders, battery_dbg = storage_policy(
        cfg, obj_agg['storages'], balance_now, useful_now, future,
        safe_float(state.get('fill_ratio_ewma', 0.74), 0.74), tick, game_length,
        loss_ratio=safe_float(state.get('loss_ratio_ewma', 0.18), 0.18),
        profile_ctx=profile_ctx,
    )

    marketable_useful_now = max(0.0, useful_now + battery_dbg['discharge_for_market'] - battery_dbg['charge_total'])
    offer_cap = compute_offer_cap(state, cfg, tick, marketable_useful_now)
    reserve = compute_reserve(state, future, object_rows, total_losses, marketable_useful_now, profile_ctx=profile_ctx)
    sell_target = max(0.0, min(offer_cap, marketable_useful_now) - reserve)
    if marketable_useful_now >= MIN_ORDER_VOLUME and sell_target < MIN_ORDER_VOLUME:
        sell_target = min(offer_cap, marketable_useful_now, max(MIN_ORDER_VOLUME, 0.60 * marketable_useful_now))
    sell_volume = round_vol(clamp(sell_target, 0.0, min(offer_cap, marketable_useful_now)))

    ladder = build_ladder(
        sell_volume,
        safe_float(state.get('market_ref', 7.2), 7.2),
        safe_float(state.get('fill_ratio_ewma', 0.74), 0.74),
        safe_int(cfg['exchangeMaxTickets'], 100),
        cfg,
        buy_ref=buy_ref,
        profile_ctx=profile_ctx,
    )

    if hasattr(psm, 'orders'):
        for sid, amount in charge_orders:
            if amount > 0.0:
                psm.orders.charge(sid, amount)
        for sid, amount in discharge_orders:
            if amount > 0.0:
                psm.orders.discharge(sid, amount)
        for volume, price in ladder:
            psm.orders.sell(volume, price)

    cumulative = update_cumulative_state(state, obj_agg, object_rows, total_generated, total_consumed, total_losses, useful_now, market_stats)
    forecast_meta = forecast_bundle.get('_meta', {})
    score_income, score_loss_only, total_score = get_score_breakdown(psm)
    summary_row = {
        'tick': tick,
        'game_length': game_length,
        'score_delta': round(score_income - score_loss_only, 6),
        'score_income': round(score_income, 6),
        'score_loss_only': round(score_loss_only, 6),
        'total_score': total_score,
        'wind_now': round(weather['wind'], 6),
        'sun_now': round(weather['sun'], 6),
        'forecast_source': forecast_meta.get('source'),
        'forecast_path': forecast_meta.get('path'),
        'forecast_rows': forecast_meta.get('rows'),
        'solar_actual': round(safe_float(obj_agg['by_type'].get('solar', {}).get('generated', 0.0), 0.0), 6),
        'wind_actual': round(safe_float(obj_agg['by_type'].get('wind', {}).get('generated', 0.0), 0.0), 6),
        'solar_theoretical_now': round(current_theoretical['solar_theoretical_now'], 6),
        'wind_theoretical_now': round(current_theoretical['wind_theoretical_now'], 6),
        'gross_theoretical_now': round(current_theoretical['gross_theoretical_now'], 6),
        'load_forecast_now': round(current_theoretical['load_forecast_now'], 6),
        'load_model_now': round(current_theoretical['load_model_now'], 6),
        'total_generated': round(total_generated, 6),
        'total_consumed': round(total_consumed, 6),
        'total_external': round(total_external, 6),
        'total_losses': round(total_losses, 6),
        'physical_balance_now': round(balance_now, 6),
        'useful_supply_now': round(useful_now, 6),
        'marketable_useful_now': round(marketable_useful_now, 6),
        'network_losses_total': round(net_agg['losses_total'], 6),
        'object_income_total': round(obj_agg['income_total'], 6),
        'object_loss_total': round(obj_agg['loss_total'], 6),
        'sell_volume': round(sell_volume, 6),
        'offer_cap': round(offer_cap, 6),
        'reserve': round(reserve, 6),
        'charge_total': round(battery_dbg['charge_total'], 6),
        'discharge_total': round(battery_dbg['discharge_total'], 6),
        'target_soc': round(battery_dbg['target_soc'], 6),
        'total_soc': round(battery_dbg['total_soc'], 6),
        'market_ref': round(safe_float(state.get('market_ref', 4.8), 4.8), 6),
        'fill_ratio_ewma': round(safe_float(state.get('fill_ratio_ewma', 0.74), 0.74), 6),
        'buy_asked': round(safe_float(market_stats.get('buy_asked', 0.0), 0.0), 6),
        'buy_contracted': round(safe_float(market_stats.get('buy_contracted', 0.0), 0.0), 6),
        'buy_instant': round(safe_float(market_stats.get('buy_instant', 0.0), 0.0), 6),
        'buy_avg_asked_price': market_stats.get('buy_avg_asked_price'),
        'buy_avg_contracted_price': market_stats.get('buy_avg_contracted_price'),
        'sell_asked': round(safe_float(market_stats.get('sell_asked', 0.0), 0.0), 6),
        'sell_contracted': round(safe_float(market_stats.get('sell_contracted', 0.0), 0.0), 6),
        'sell_instant': round(safe_float(market_stats.get('sell_instant', 0.0), 0.0), 6),
        'sell_avg_asked_price': market_stats.get('sell_avg_asked_price'),
        'sell_avg_contracted_price': market_stats.get('sell_avg_contracted_price'),
        'ladder': ladder,
        'topology_warnings': topology.get('warnings', []),
        'type_totals': {
            typ: {
                'count': data['count'],
                'generated': round(data['generated'], 6),
                'consumed': round(data['consumed'], 6),
                'income': round(data['income'], 6),
                'loss': round(data['loss'], 6),
            } for typ, data in obj_agg['by_type'].items()
        },
    }
    strategy_row = {
        'tick': tick,
        'weather': weather,
        'forecast_meta': forecast_meta,
        'current_theoretical': current_theoretical,
        'market_book': market_stats,
        'config_core': {
            'exchangeExternalSell': cfg['exchangeExternalSell'],
            'exchangeExternalBuy': cfg['exchangeExternalBuy'],
            'exchangeExternalInstantSell': cfg['exchangeExternalInstantSell'],
            'exchangeExternalInstantBuy': cfg['exchangeExternalInstantBuy'],
            'exchangeAmountScaler': cfg['exchangeAmountScaler'],
            'exchangeAmountBuffer': cfg['exchangeAmountBuffer'],
            'cellCapacity': cfg['cellCapacity'],
            'cellChargeRate': cfg['cellChargeRate'],
            'cellDischargeRate': cfg['cellDischargeRate'],
            'weatherEffectsDelay': cfg['weatherEffectsDelay'],
        },
        'models': state.get('object_models', {}),
        'future_window': future,
        'topology': topology,
        'physical_balance_now': balance_now,
        'useful_supply_now': useful_now,
        'marketable_useful_now': marketable_useful_now,
        'reserve_formula': {
            'abs_err_ewma': safe_float(state.get('abs_err_ewma', 1.2), 1.2),
            'loss_ratio_ewma': safe_float(state.get('loss_ratio_ewma', 0.18), 0.18),
            'reserve': reserve,
        },
        'offer_cap': offer_cap,
        'battery': battery_dbg,
        'charge_orders': charge_orders,
        'discharge_orders': discharge_orders,
        'sell_volume': sell_volume,
        'ladder': ladder,
    }
    derived_row = {
        'tick': tick,
        'sun_now': round(weather['sun'], 6),
        'wind_now': round(weather['wind'], 6),
        'forecast_source': forecast_meta.get('source'),
        'forecast_rows': forecast_meta.get('rows'),
        'solar_actual': round(safe_float(obj_agg['by_type'].get('solar', {}).get('generated', 0.0), 0.0), 6),
        'wind_actual': round(safe_float(obj_agg['by_type'].get('wind', {}).get('generated', 0.0), 0.0), 6),
        'gross_actual': round(total_generated, 6),
        'solar_theoretical_now': round(current_theoretical['solar_theoretical_now'], 6),
        'wind_theoretical_now': round(current_theoretical['wind_theoretical_now'], 6),
        'gross_theoretical_now': round(current_theoretical['gross_theoretical_now'], 6),
        'load_forecast_now': round(current_theoretical['load_forecast_now'], 6),
        'load_actual_now': round(total_consumed, 6),
        'useful_energy_now': round(useful_now, 6),
        'physical_balance_now': round(balance_now, 6),
        'marketable_useful_now': round(marketable_useful_now, 6),
        'losses_now': round(total_losses, 6),
        'buy_asked': round(safe_float(market_stats.get('buy_asked', 0.0), 0.0), 6),
        'buy_contracted': round(safe_float(market_stats.get('buy_contracted', 0.0), 0.0), 6),
        'buy_instant': round(safe_float(market_stats.get('buy_instant', 0.0), 0.0), 6),
        'buy_avg_asked_price': market_stats.get('buy_avg_asked_price'),
        'buy_avg_contracted_price': market_stats.get('buy_avg_contracted_price'),
        'sell_asked': round(safe_float(market_stats.get('sell_asked', 0.0), 0.0), 6),
        'sell_contracted': round(safe_float(market_stats.get('sell_contracted', 0.0), 0.0), 6),
        'sell_instant': round(safe_float(market_stats.get('sell_instant', 0.0), 0.0), 6),
        'sell_avg_asked_price': market_stats.get('sell_avg_asked_price'),
        'sell_avg_contracted_price': market_stats.get('sell_avg_contracted_price'),
        'storage_soc_total': round(battery_dbg['total_soc'], 6),
        'storage_target_soc': round(battery_dbg['target_soc'], 6),
        'storage_prep_soc': round(safe_float(battery_dbg.get('prep_soc', battery_dbg['target_soc']), battery_dbg['target_soc']), 6),
        'storage_charge_total': round(battery_dbg['charge_total'], 6),
        'storage_discharge_total': round(battery_dbg['discharge_total'], 6),
        'offer_cap': round(offer_cap, 6),
        'reserve': round(reserve, 6),
        'sell_volume': round(sell_volume, 6),
        'topology_warning_count': len(topology.get('warnings', [])),
        'forecast_profile_context': profile_ctx,
        'cum_solar_actual': round(safe_float(cumulative.get('solar_generated', 0.0), 0.0), 6),
        'cum_wind_actual': round(safe_float(cumulative.get('wind_generated', 0.0), 0.0), 6),
        'cum_gross_actual': round(safe_float(cumulative.get('gross_generated', 0.0), 0.0), 6),
        'cum_load_actual': round(safe_float(cumulative.get('gross_consumed', 0.0), 0.0), 6),
        'cum_losses': round(safe_float(cumulative.get('losses', 0.0), 0.0), 6),
        'cum_useful_energy': round(safe_float(cumulative.get('useful_energy', 0.0), 0.0), 6),
        'cum_buy_asked': round(safe_float(cumulative.get('buy_asked', 0.0), 0.0), 6),
        'cum_buy_contracted': round(safe_float(cumulative.get('buy_contracted', 0.0), 0.0), 6),
        'cum_sell_asked': round(safe_float(cumulative.get('sell_asked', 0.0), 0.0), 6),
        'cum_sell_contracted': round(safe_float(cumulative.get('sell_contracted', 0.0), 0.0), 6),
    }
    log_tick_data(tick, state, object_rows, network_rows, exchange_rows, summary_row, strategy_row, derived_row, object_prediction_rows)
    state['prev_useful_supply_est'] = marketable_useful_now
    state['last_sell_volume'] = sell_volume
    save_state(state)
    return summary_row


def main() -> None:
    ensure_dir(LOG_DIR)
    psm = ips.init()
    try:
        summary = controller(psm)
        print(json.dumps(summary, ensure_ascii=False))
    except Exception as e:
        err = {'tick': get_tick(psm), 'error': str(e), 'traceback': traceback.format_exc()}
        write_jsonl(STRATEGY_DEBUG_FILE, err)
        print(json.dumps(err, ensure_ascii=False))
    if hasattr(psm, 'save_and_exit'):
        psm.save_and_exit()


if __name__ == '__main__':
    main()
