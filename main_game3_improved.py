import csv
import glob
import json
import math
import os
import traceback
from typing import Any, Dict, List, Optional, Tuple

import ips

STATE_FILE = os.path.expanduser('~/ips3-sandbox/ies_state.json')
LOG_DIR = os.path.expanduser('~/ips3-sandbox/ies_logs')
TICK_SUMMARY_FILE = os.path.join(LOG_DIR, 'tick_summary.jsonl')
STRATEGY_DEBUG_FILE = os.path.join(LOG_DIR, 'strategy_debug.jsonl')
OBJECTS_CSV = os.path.join(LOG_DIR, 'objects_timeseries.csv')
NETWORKS_CSV = os.path.join(LOG_DIR, 'networks_timeseries.csv')
EXCHANGE_CSV = os.path.join(LOG_DIR, 'exchange_timeseries.csv')
FORECAST_ERRORS_CSV = os.path.join(LOG_DIR, 'forecast_errors.csv')
DERIVED_METRICS_CSV = os.path.join(LOG_DIR, 'derived_metrics.csv')

MIN_ORDER_VOLUME = 0.25
MIN_RESERVE = 0.8
SOC_FLOOR_FRAC = 0.06
SOC_CEIL_FRAC = 0.95
ENDGAME_TICKS = 5
LOOKAHEAD = 6
STRICT_FIRST_TICK_CAP = True

# Seeds learned from previous game logs.
# They are only priors: if addresses differ, generic defaults are used and
# the model re-learns online from the current game.
SOLAR_SEED_FACTORS = {
    's1': 0.50,
    's7': 0.77,
    's8': 0.86,
}
WIND_SEED_FACTORS = {
    'a6': 0.0054,
    'a8': 0.0047,
}
LOAD_BIAS_PRIOR = 0.82
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
_FORECAST_CACHE = {'key': None, 'payload': None}


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
    pred = (gq * total_gen * total_gen + gl * total_gen +
            lq * total_load * total_load + ll * total_load + base)
    pred = max(0.0, pred)
    return pred * clamp(scale, 0.6, 1.6)


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
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception:
        pass




def normalize_forecast_header(name: Any) -> str:
    s = str(name or '').strip().lower().replace('ё', 'е').replace('_', ' ').replace('-', ' ')
    s = ' '.join(s.split())
    return FORECAST_HEADER_TO_KEY.get(s, '')


def find_external_forecast_file() -> Optional[str]:
    env_path = os.environ.get('IES_FORECAST_FILE')
    candidates = []
    if env_path:
        candidates.append(env_path)

    search_roots = [
        os.getcwd(),
        os.path.expanduser('~/ips3-sandbox'),
        '/mnt/data',
    ]
    for root in search_roots:
        try:
            if not os.path.isdir(root):
                continue
            for pattern in ('forecast*.csv', '*forecast*.csv', '*прогноз*.csv'):
                candidates.extend(glob.glob(os.path.join(root, pattern)))
        except Exception:
            continue

    uniq = []
    seen = set()
    for cand in candidates:
        full = os.path.abspath(os.path.expanduser(cand))
        if full in seen or not os.path.isfile(full):
            continue
        seen.add(full)
        uniq.append(full)

    if not uniq:
        return None

    uniq.sort(key=lambda p: (os.path.getmtime(p), os.path.getsize(p), p), reverse=True)
    return uniq[0]


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

    series = {name: [] for name in FORECAST_SERIES_ORDER}
    rows_read = 0

    try:
        with open(path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                payload = None
            else:
                header_map = {field: normalize_forecast_header(field) for field in reader.fieldnames}
                for row in reader:
                    if not any((str(v).strip() for v in row.values())):
                        continue
                    rows_read += 1
                    normalized = {}
                    for raw_key, value in row.items():
                        canon = header_map.get(raw_key, '')
                        if canon:
                            normalized[canon] = safe_float(value, 0.0)
                    for name in FORECAST_SERIES_ORDER:
                        if name in normalized:
                            series[name].append(normalized[name])
                max_len = max((len(v) for v in series.values()), default=0)
                if max_len <= 0:
                    payload = None
                else:
                    payload = {
                        'path': path,
                        'rows': rows_read,
                        'bundle': {
                            name: {'data': list(values), 'spread': 0.0}
                            for name, values in series.items()
                            if values
                        },
                    }
    except Exception:
        payload = None

    _FORECAST_CACHE['key'] = cache_key
    _FORECAST_CACHE['payload'] = payload
    return payload

def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


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
    return str(address)


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


def get_score_delta(psm: Any) -> float:
    raw = psm.get('scoreDelta') if isinstance(psm, dict) else getattr(psm, 'scoreDelta', 0.0)
    if isinstance(raw, list):
        return safe_float(raw[0], 0.0)
    return safe_float(raw, 0.0)


def get_total_score(psm: Any) -> Optional[float]:
    raw = psm.get('scoreDelta') if isinstance(psm, dict) else getattr(psm, 'scoreDelta', None)
    if isinstance(raw, list) and len(raw) > 1:
        return safe_float(raw[1], 0.0)
    return None


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


def get_weather_then(psm: Any, name: str) -> List[float]:
    raw = psm.get(name) if isinstance(psm, dict) else getattr(psm, name, None)
    if isinstance(raw, list):
        return list(raw[1]) if len(raw) > 1 else []
    return list(getattr(raw, 'then', []))


def get_forecast_bundle(psm: Any) -> Dict[str, Dict[str, Any]]:
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

    external = load_external_forecast_csv()
    if external:
        for name, item in external['bundle'].items():
            spread = safe_float(out.get(name, {}).get('spread', 0.0), 0.0)
            out[name] = {
                'data': list(item.get('data', [])),
                'spread': spread,
                'source': 'csv',
            }
        out['_meta'] = {
            'source': 'csv',
            'path': external['path'],
            'rows': external['rows'],
        }
    else:
        rows = max((len(v.get('data', [])) for v in out.values() if isinstance(v, dict) and 'data' in v), default=0)
        out['_meta'] = {
            'source': 'psm',
            'path': None,
            'rows': rows,
        }
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
    out = {}
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


def obj_storage_order_id(obj: Any) -> str:
    address = obj_address(obj)
    if address:
        return str(address[0])
    oid = obj_id(obj)
    if isinstance(oid, list) and len(oid) > 1:
        return f"c{oid[1]}"
    return str(oid)


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
        'prev_useful': None,
        'last_sell_volume': 0.0,
        'abs_err_ewma': 1.5,
        'loss_ratio_ewma': 0.18,
        'fill_ratio_ewma': 0.72,
        'market_ref': 4.8,
        'load_bias_total': LOAD_BIAS_PRIOR,
        'load_abs_err': 2.0,
        'object_models': {},
        'last_forecast_actuals': {},
        'loss_model': dict(LOSS_MODEL_PRIOR, scale=1.0),
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




def get_model(state: Dict[str, Any], key: str, kind: str) -> Dict[str, Any]:
    models = state.setdefault('object_models', {})
    if key not in models:
        if kind == 'solar':
            models[key] = {
                'kind': kind,
                'factor': SOLAR_SEED_FACTORS.get(key, 0.65),
                'err': 0.8,
            }
        elif kind == 'wind':
            models[key] = {
                'kind': kind,
                'factor': WIND_SEED_FACTORS.get(key, 0.0050),
                'rot_factor': 80.0,
                'wind_to_rot': 0.040,
                'err': 2.5,
                'last_failed': 0,
            }
        else:
            models[key] = {'kind': kind, 'bias': 1.0, 'err': 0.6}
    return models[key]


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
    info = {
        'gen_total': 0.0,
        'cons_total': 0.0,
        'income_total': 0.0,
        'loss_total': 0.0,
        'by_type': {},
        'storages': [],
        'generators': [],
        'consumers': [],
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
            info['storages'].append({
                'id': row['address'].split('|')[0],
                'soc': safe_float(row['charge_now'], 0.0),
            })
        if row['type'] in ('solar', 'wind'):
            info['generators'].append(row)
        if row['type'] in ('houseA', 'houseB', 'office', 'factory', 'hospital'):
            info['consumers'].append(row)
    return info


def aggregate_networks(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    return {
        'upflow_total': sum(r['upflow'] for r in rows),
        'downflow_total': sum(r['downflow'] for r in rows),
        'losses_total': sum(r['losses'] for r in rows),
    }


def log_tick_data(
    tick: int,
    object_rows: List[Dict[str, Any]],
    network_rows: List[Dict[str, Any]],
    exchange_rows: List[Dict[str, Any]],
    summary_row: Dict[str, Any],
    strategy_row: Dict[str, Any],
    derived_row: Optional[Dict[str, Any]] = None,
    object_prediction_rows: Optional[List[Dict[str, Any]]] = None,
) -> None:
    ensure_dir(LOG_DIR)
    write_jsonl(TICK_SUMMARY_FILE, summary_row)
    write_jsonl(STRATEGY_DEBUG_FILE, strategy_row)

    for row in object_rows:
        csv_row = {'tick': tick}
        csv_row.update(row)
        append_csv(OBJECTS_CSV, [
            'tick', 'id', 'type', 'contract', 'address', 'path',
            'generated', 'consumed', 'income', 'loss',
            'charge_now', 'wind_rotation', 'failed',
        ], csv_row)

    for row in network_rows:
        csv_row = {'tick': tick}
        csv_row.update(row)
        append_csv(NETWORKS_CSV, [
            'tick', 'network_index', 'location', 'upflow', 'downflow', 'losses'
        ], csv_row)

    for idx, row in enumerate(exchange_rows):
        csv_row = {'tick': tick, 'receipt_index': idx}
        csv_row.update(row)
        append_csv(EXCHANGE_CSV, [
            'tick', 'receipt_index', 'side', 'askedAmount', 'askedPrice',
            'contractedAmount', 'contractedPrice', 'instantAmount'
        ], csv_row)

    if derived_row is not None:
        append_csv(DERIVED_METRICS_CSV, [
            'tick',
            'sun_now', 'wind_now',
            'solar_actual', 'wind_actual', 'gross_actual',
            'solar_theoretical_now', 'wind_theoretical_now', 'gross_theoretical_now',
            'load_forecast_now', 'load_actual_now',
            'useful_energy_now', 'exportable_now', 'losses_now',
            'buy_asked', 'buy_contracted', 'buy_instant', 'buy_avg_asked_price', 'buy_avg_contracted_price',
            'sell_asked', 'sell_contracted', 'sell_instant', 'sell_avg_asked_price', 'sell_avg_contracted_price',
            'storage_soc_total', 'storage_target_soc', 'storage_charge_total', 'storage_discharge_total',
            'offer_cap', 'reserve', 'sell_volume',
            'cum_solar_actual', 'cum_wind_actual', 'cum_gross_actual', 'cum_load_actual', 'cum_losses', 'cum_useful_energy',
            'cum_buy_asked', 'cum_buy_contracted', 'cum_sell_asked', 'cum_sell_contracted'
        ], derived_row)

    if object_prediction_rows:
        for row in object_prediction_rows:
            append_csv(os.path.join(LOG_DIR, 'object_predictions.csv'), [
                'tick', 'address', 'type',
                'gen_actual', 'gen_theoretical_now', 'gen_model_error',
                'load_actual', 'load_forecast_now', 'load_model_now', 'load_model_error',
                'sun_now', 'wind_now'
            ], row)


# =====================
# Forecast and learning
# =====================

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



def update_forecast_error_log(
    tick: int,
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    weather: Dict[str, float],
    bundle: Dict[str, Dict[str, Any]],
    total_consumed: float,
) -> None:
    prev_key = str(tick)
    entry = state.get('last_forecast_actuals', {}).get(prev_key)
    if entry:
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
        for name, fc in entry.items():
            actual = actuals.get(name, 0.0)
            append_csv(FORECAST_ERRORS_CSV, [
                'tick', 'metric', 'forecast', 'actual', 'error', 'abs_error'
            ], {
                'tick': tick,
                'metric': name,
                'forecast': round(fc, 6),
                'actual': round(actual, 6),
                'error': round(actual - fc, 6),
                'abs_error': round(abs(actual - fc), 6),
            })
        state['last_forecast_actuals'].pop(prev_key, None)


def store_current_forecast_for_next_tick(
    tick: int,
    state: Dict[str, Any],
    bundle: Dict[str, Dict[str, Any]],
    object_rows: List[Dict[str, Any]],
) -> None:
    state.setdefault('last_forecast_actuals', {})[str(tick)] = {
        'hospital': get_forecast_value(bundle, 'hospital', tick),
        'factory': get_forecast_value(bundle, 'factory', tick),
        'office': get_forecast_value(bundle, 'office', tick),
        'houseA': get_forecast_value(bundle, 'houseA', tick),
        'houseB': get_forecast_value(bundle, 'houseB', tick),
        'sun': get_forecast_value(bundle, 'sun', tick),
        'wind': get_forecast_value(bundle, 'wind', tick),
        'total_load': aggregate_forecast_load(bundle, object_rows, tick),
    }
    if len(state['last_forecast_actuals']) > 20:
        keys = sorted(state['last_forecast_actuals'].keys(), key=lambda x: int(x))
        for k in keys[:-20]:
            state['last_forecast_actuals'].pop(k, None)



def update_models(
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    weather: Dict[str, float],
    bundle: Dict[str, Dict[str, Any]],
    tick: int,
    cfg: Optional[Dict[str, Any]] = None,
    total_consumed: Optional[float] = None,
    total_losses: Optional[float] = None,
) -> None:
    sun_now = max(0.0, weather['sun'])
    wind_now = max(0.0, weather['wind'])

    total_fc_now = aggregate_forecast_load(bundle, object_rows, tick)
    if total_consumed is not None and total_fc_now > 1e-6:
        load_bias = total_consumed / max(total_fc_now, 1e-6)
        load_bias = clamp(load_bias, 0.70, 1.05)
        state['load_bias_total'] = 0.92 * safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) + 0.08 * load_bias
        pred_total = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) * total_fc_now
        state['load_abs_err'] = 0.90 * safe_float(state.get('load_abs_err', 2.0), 2.0) + 0.10 * abs(total_consumed - pred_total)

    if total_consumed is not None and total_losses is not None:
        pred_loss = predict_total_losses(state, sum(r['generated'] for r in object_rows), total_consumed)
        if pred_loss > 1e-6:
            scale = total_losses / pred_loss
            lm = state.setdefault('loss_model', dict(LOSS_MODEL_PRIOR))
            lm['scale'] = 0.90 * safe_float(lm.get('scale', 1.0), 1.0) + 0.10 * clamp(scale, 0.6, 1.6)

    for row in object_rows:
        key = row['address']
        typ = row['type']
        if typ == 'solar':
            model = get_model(state, key, 'solar')
            actual = row['generated']
            if sun_now > 0.08 and actual >= 0.0:
                est = actual / max(sun_now, 1e-6)
                model['factor'] = 0.88 * safe_float(model.get('factor', 0.65), 0.65) + 0.12 * clamp(est, 0.0, 1.5)
            pred = safe_float(model.get('factor', 0.65), 0.65) * sun_now
            model['err'] = 0.88 * safe_float(model.get('err', 0.8), 0.8) + 0.12 * abs(actual - pred)

        elif typ == 'wind':
            model = get_model(state, key, 'wind')
            actual = row['generated']
            rotation_now = max(0.0, safe_float(row.get('wind_rotation', 0.0), 0.0))
            failed_now = safe_int(row.get('failed', 0), 0)

            if wind_now > 0.2 and actual >= 0.0:
                est = actual / max(wind_now ** 3, 1e-6)
                model['factor'] = 0.92 * safe_float(model.get('factor', 0.0050), 0.0050) + 0.08 * clamp(est, 0.0, 0.02)

            if wind_now > 0.2 and rotation_now > 0.03:
                ratio = rotation_now / max(wind_now, 1e-6)
                model['wind_to_rot'] = 0.92 * safe_float(model.get('wind_to_rot', 0.040), 0.040) + 0.08 * clamp(ratio, 0.015, 0.080)

            if rotation_now > 0.05 and actual >= 0.0:
                est_rot = actual / max(rotation_now ** 3, 1e-6)
                model['rot_factor'] = 0.90 * safe_float(model.get('rot_factor', 80.0), 80.0) + 0.10 * clamp(est_rot, 20.0, 160.0)

            pred_direct = safe_float(model.get('factor', 0.0050), 0.0050) * (wind_now ** 3)
            pred_rot = safe_float(model.get('rot_factor', 80.0), 80.0) * (rotation_now ** 3)
            pred = 0.55 * pred_direct + 0.45 * pred_rot
            if failed_now > 0:
                pred *= 0.85
            model['err'] = 0.90 * safe_float(model.get('err', 2.5), 2.5) + 0.10 * abs(actual - pred)
            model['last_failed'] = failed_now

        elif typ in OBJECT_TYPE_TO_FORECAST:
            model = get_model(state, key, 'load')
            actual = row['consumed']
            fc_name = OBJECT_TYPE_TO_FORECAST.get(typ)
            fc_now = get_forecast_value(bundle, fc_name, tick)
            base_bias = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
            if fc_now > 0.05 and actual >= 0.0:
                est_bias = actual / max(fc_now, 1e-6)
                model['bias'] = 0.96 * safe_float(model.get('bias', base_bias), base_bias) + 0.04 * clamp(est_bias, 0.60, 1.10)
            else:
                model['bias'] = 0.98 * safe_float(model.get('bias', base_bias), base_bias) + 0.02 * base_bias
            pred = safe_float(model.get('bias', base_bias), base_bias) * max(fc_now, 0.0)
            model['err'] = 0.92 * safe_float(model.get('err', 0.6), 0.6) + 0.08 * abs(actual - pred)



def predict_object_generation(
    state: Dict[str, Any],
    row: Dict[str, Any],
    fc_sun: float,
    fc_wind: float,
    sun_spread: float,
    wind_spread: float,
    cfg: Optional[Dict[str, Any]] = None,
    weather: Optional[Dict[str, Any]] = None,
) -> float:
    key = row['address']
    typ = row['type']

    if typ == 'solar':
        model = get_model(state, key, 'solar')
        safe_sun = max(0.0, fc_sun - 0.25 * sun_spread)
        return clamp(
            safe_float(model.get('factor', 0.65), 0.65) * safe_sun,
            0.0,
            safe_float((cfg or {}).get('maxSolarPower', 20.0), 20.0),
        )

    if typ == 'wind':
        model = get_model(state, key, 'wind')
        current_wind = max(0.0, safe_float((weather or {}).get('wind', 0.0), 0.0))
        current_rot = max(0.0, safe_float(row.get('wind_rotation', 0.0), 0.0))
        max_wind_power = safe_float((cfg or {}).get('maxWindPower', 20.0), 20.0)

        safe_wind = max(0.0, fc_wind - 0.30 * wind_spread)
        direct = safe_float(model.get('factor', 0.0050), 0.0050) * (safe_wind ** 3)

        proj_rot = 0.0
        if safe_wind > 0.0:
            rot_ratio = safe_float(model.get('wind_to_rot', 0.040), 0.040)
            if current_wind > 0.2 and current_rot > 0.02:
                inertia = 0.70
                proj_rot = inertia * current_rot + (1.0 - inertia) * rot_ratio * safe_wind
            else:
                proj_rot = rot_ratio * safe_wind
        rot_based = safe_float(model.get('rot_factor', 80.0), 80.0) * max(0.0, proj_rot) ** 3

        pred = 0.65 * direct + 0.35 * rot_based
        if safe_int(row.get('failed', 0), 0) > 0:
            pred *= 0.82
        elif current_wind > 0.2 and safe_wind > 1.10 * current_wind:
            pred *= 0.93

        return clamp(pred, 0.0, max_wind_power)

    return 0.0



def predict_object_load(state: Dict[str, Any], row: Dict[str, Any], forecast_value: float) -> float:
    key = row['address']
    typ = row['type']
    if typ not in OBJECT_TYPE_TO_FORECAST:
        return 0.0
    model = get_model(state, key, 'load')
    base_bias = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
    bias = 0.35 * safe_float(model.get('bias', base_bias), base_bias) + 0.65 * base_bias
    return max(0.0, forecast_value * bias)




def forecast_window(
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    bundle: Dict[str, Dict[str, Any]],
    tick: int,
    game_length: int,
    horizon: int,
) -> List[Dict[str, Any]]:
    wind_spread = get_forecast_spread(bundle, 'wind', 0.0)
    sun_spread = get_forecast_spread(bundle, 'sun', 0.0)
    out = []

    counts = count_forecast_objects(object_rows)
    base_bias = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
    last_tick = max(0, game_length - 1)
    for t in range(tick, min(last_tick, tick + horizon - 1) + 1):
        fc_sun = get_forecast_value(bundle, 'sun', t)
        fc_wind = get_forecast_value(bundle, 'wind', t)
        per_object = []
        total_gen = 0.0
        total_load = 0.0
        type_totals = {}

        for row in object_rows:
            gen_pred = predict_object_generation(
                state, row, fc_sun, fc_wind, sun_spread, wind_spread,
                cfg=state.get('cfg_runtime', {}), weather=state.get('weather_runtime', {})
            )
            total_gen += gen_pred
            type_totals.setdefault(row['type'], {'gen': 0.0, 'load': 0.0})
            type_totals[row['type']]['gen'] += gen_pred
            per_object.append({
                'address': row['address'],
                'type': row['type'],
                'gen_pred': gen_pred,
                'load_pred': 0.0,
            })

        for typ, fc_name in OBJECT_TYPE_TO_FORECAST.items():
            cnt = counts.get(typ, 0)
            if cnt <= 0:
                continue
            fc_val = get_forecast_value(bundle, fc_name, t)
            total_type_load = cnt * fc_val * base_bias
            total_load += total_type_load
            type_totals.setdefault(typ, {'gen': 0.0, 'load': 0.0})
            type_totals[typ]['load'] += total_type_load
            per_obj_load = total_type_load / cnt
            for item in per_object:
                if item['type'] == typ:
                    item['load_pred'] = per_obj_load

        loss_pred = predict_total_losses(state, total_gen, total_load)
        useful_pred = total_gen - total_load - loss_pred
        out.append({
            'tick': t,
            'sun': fc_sun,
            'wind': fc_wind,
            'total_gen_pred': total_gen,
            'total_load_pred': total_load,
            'total_loss_pred': loss_pred,
            'useful_pred': useful_pred,
            'per_object': per_object,
            'type_totals': type_totals,
        })
    return out


def update_cumulative_state(
    state: Dict[str, Any],
    obj_agg: Dict[str, Any],
    total_generated: float,
    total_consumed: float,
    total_losses: float,
    useful_now: float,
    market_stats: Dict[str, Any],
) -> Dict[str, Any]:
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
    return cumulative


# =====================
# Exchange and strategy
# =====================

def weighted_avg(weighted_sum: float, volume: float) -> Optional[float]:
    if volume > 1e-9:
        return weighted_sum / volume
    return None


def analyze_exchange(exchange_rows: List[Dict[str, float]]) -> Dict[str, Any]:
    stats = {
        'buy': {
            'asked': 0.0,
            'contracted': 0.0,
            'instant': 0.0,
            'weighted_asked': 0.0,
            'weighted_contracted': 0.0,
        },
        'sell': {
            'asked': 0.0,
            'contracted': 0.0,
            'instant': 0.0,
            'weighted_asked': 0.0,
            'weighted_contracted': 0.0,
        },
        'flat': {
            'asked': 0.0,
            'contracted': 0.0,
            'instant': 0.0,
            'weighted_asked': 0.0,
            'weighted_contracted': 0.0,
        },
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
    return total_generated - total_consumed - total_losses


def compute_offer_cap(state: Dict[str, Any], cfg: Dict[str, Any], tick: int, exportable_now: float) -> float:
    prev_export = state.get('prev_exportable')
    if prev_export is None:
        if tick == 0 and STRICT_FIRST_TICK_CAP:
            return safe_float(cfg['exchangeAmountBuffer'], 10.0)
        return max(safe_float(cfg['exchangeAmountBuffer'], 10.0), exportable_now)
    return safe_float(prev_export, 0.0) * safe_float(cfg['exchangeAmountScaler'], 1.2) + safe_float(cfg['exchangeAmountBuffer'], 10.0)




def compute_target_soc(
    cfg: Dict[str, Any],
    total_capacity: float,
    total_soc: float,
    future: List[Dict[str, Any]],
    fill_ratio: float,
    tick: int,
    game_length: int,
    loss_ratio: float = 0.10,
) -> float:
    base_ceil = total_capacity * SOC_CEIL_FRAC

    weighted_gap = 0.0
    weighted_surplus = 0.0
    raw_gap_sum = 0.0
    raw_surplus_sum = 0.0
    for i, row in enumerate(future):
        w = 1.0 / (i + 1)
        gap = max(0.0, -safe_float(row.get('useful_pred', 0.0), 0.0))
        surplus = max(0.0, safe_float(row.get('useful_pred', 0.0), 0.0))
        weighted_gap += w * gap
        weighted_surplus += w * surplus
        raw_gap_sum += gap
        raw_surplus_sum += surplus

    chronic_deficit = raw_gap_sum > max(5.0, 2.0 * raw_surplus_sum)
    floor = total_capacity * (0.02 if chronic_deficit else SOC_FLOOR_FRAC)
    target = floor + 0.28 * weighted_gap - 0.75 * weighted_surplus

    if fill_ratio < 0.65 and raw_surplus_sum > 0.0:
        target += 0.04 * total_capacity
    elif fill_ratio > 0.92 and raw_surplus_sum > raw_gap_sum:
        target -= 0.03 * total_capacity

    ticks_left = game_length - tick
    if ticks_left <= ENDGAME_TICKS:
        floor = total_capacity * 0.0
        target = floor + 0.08 * weighted_gap

    return clamp(target, floor, base_ceil)



def storage_policy(
    cfg: Dict[str, Any],
    storages: List[Dict[str, Any]],
    net_now: float,
    future: List[Dict[str, Any]],
    fill_ratio: float,
    tick: int,
    game_length: int,
    loss_ratio: float = 0.10,
) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]], Dict[str, Any]]:
    if not storages:
        return [], [], {
            'target_soc': 0.0,
            'total_soc': 0.0,
            'charge_total': 0.0,
            'discharge_total': 0.0,
        }

    cell_capacity = safe_float(cfg['cellCapacity'], 120.0)
    charge_rate = safe_float(cfg['cellChargeRate'], 15.0)
    discharge_rate = safe_float(cfg['cellDischargeRate'], 20.0)
    total_capacity = len(storages) * cell_capacity
    total_soc = sum(s['soc'] for s in storages)
    total_charge_rate = len(storages) * charge_rate
    total_discharge_rate = len(storages) * discharge_rate

    target_soc = compute_target_soc(
        cfg, total_capacity, total_soc, future, fill_ratio, tick, game_length, loss_ratio=loss_ratio
    )

    deficit_sum = sum(max(0.0, -safe_float(r.get('useful_pred', 0.0), 0.0)) for r in future)
    surplus_sum = sum(max(0.0, safe_float(r.get('useful_pred', 0.0), 0.0)) for r in future)
    next_deficit = max(0.0, -safe_float(future[0].get('useful_pred', 0.0), 0.0)) if future else 0.0

    chronic_deficit = deficit_sum > max(5.0, 2.0 * surplus_sum)
    floor_frac = 0.02 if chronic_deficit else SOC_FLOOR_FRAC
    floor_soc = total_capacity * floor_frac

    charge_total = 0.0
    discharge_total = 0.0
    discharge_for_sell = 0.0

    current_surplus = max(0.0, net_now)
    current_deficit = max(0.0, -net_now)

    protected_soc = floor_soc if chronic_deficit else max(floor_soc, 0.60 * target_soc)
    discharge_budget = max(0.0, total_soc - protected_soc)

    if current_deficit > 0.0 and discharge_budget > 0.0:
        desired = current_deficit
        if chronic_deficit:
            desired += 0.20 * next_deficit
        discharge_total = min(total_discharge_rate, discharge_budget, desired)

    if current_surplus > 0.0 and total_soc < total_capacity * SOC_CEIL_FRAC:
        desired_charge = current_surplus * 0.98
        charge_room = total_capacity * SOC_CEIL_FRAC - total_soc
        if chronic_deficit or total_soc < target_soc + 0.10 * total_capacity:
            charge_total = min(total_charge_rate, charge_room, desired_charge)

    if tick >= game_length - ENDGAME_TICKS and total_soc > floor_soc:
        end_floor = total_capacity * 0.0
        discharge_for_sell = min(
            max(0.0, total_discharge_rate - discharge_total),
            max(0.0, total_soc - end_floor),
            0.55 * total_soc,
        )
        discharge_total += max(0.0, discharge_for_sell)

    if charge_total > 0 and discharge_total > 0:
        if discharge_total >= charge_total:
            charge_total = 0.0
        else:
            discharge_total = 0.0

    charge_orders = []
    discharge_orders = []

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
    end_floor_per_cell = 0.0
    for s in sorted(storages, key=lambda x: -x['soc']):
        if rem <= 1e-9:
            break
        floor_here = end_floor_per_cell if tick >= game_length - ENDGAME_TICKS else floor_per_cell
        avail = max(0.0, s['soc'] - floor_here)
        amt = min(rem, discharge_rate, avail)
        if amt >= 1e-9:
            discharge_orders.append((s['id'], round_vol(amt)))
            rem -= amt

    charge_total = sum(v for _, v in charge_orders)
    discharge_total = sum(v for _, v in discharge_orders)

    return charge_orders, discharge_orders, {
        'target_soc': target_soc,
        'total_soc': total_soc,
        'charge_total': charge_total,
        'discharge_total': discharge_total,
        'discharge_for_sell': max(0.0, discharge_for_sell),
        'chronic_deficit': chronic_deficit,
        'floor_soc': floor_soc,
    }



def compute_reserve(
    state: Dict[str, Any],
    future: List[Dict[str, Any]],
    object_rows: List[Dict[str, Any]],
    network_losses: float,
    exportable_now: float,
) -> float:
    gen_total = sum(r['generated'] for r in object_rows)
    wind_gen = sum(r['generated'] for r in object_rows if r['type'] == 'wind')
    wind_share = wind_gen / max(gen_total, 1e-9) if gen_total > 0 else 0.0

    reserve = max(
        0.30,
        0.35 * safe_float(state.get('abs_err_ewma', 1.5), 1.5),
        0.08 * network_losses,
        0.02 * max(0.0, exportable_now),
    )
    if wind_share > 0.35:
        reserve += 0.03 * wind_gen
    if future:
        next_gap = max(0.0, -safe_float(future[0].get('useful_pred', 0.0), 0.0))
        reserve += 0.02 * next_gap
    return reserve



def build_ladder(sell_volume: float, market_ref: float, fill_ratio: float, max_tickets: int, cfg: Dict[str, Any]) -> List[Tuple[float, float]]:
    if sell_volume < MIN_ORDER_VOLUME:
        return []

    market_cap = safe_float(cfg.get('exchangeExternalBuy', 10.0), 10.0)
    gp_price = safe_float(cfg.get('exchangeExternalSell', 2.0), 2.0)
    step = safe_float(cfg.get('exchangeConsumerPriceStep', 0.2), 0.2)

    # In this game surplus windows are rare; it is usually better to sell low and get a contract
    # rather than fall back to GP at the same 2 ₽/MWh.
    market_ref = clamp(market_ref, gp_price + 0.6, max(gp_price + 1.0, market_cap - 0.4))

    if sell_volume < 1.5:
        prices = [gp_price + 0.4, gp_price + 1.0]
        shares = [0.75, 0.25]
    elif fill_ratio < 0.65:
        prices = [gp_price + 0.4, gp_price + 1.0, gp_price + 2.0]
        shares = [0.70, 0.20, 0.10]
    else:
        prices = [gp_price + 0.6, min(market_ref, gp_price + 1.8), min(market_ref + 1.2, market_cap - 0.2)]
        shares = [0.58, 0.27, 0.15]

    prices = [round_price(p, price_max=market_cap, price_step=step) for p in prices]
    out = []
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


# =====================
# Config
# =====================

def normalize_cfg(raw: Dict[str, Any]) -> Dict[str, Any]:
    def g(name: str, default: float) -> float:
        return safe_float(raw.get(name, default), default)

    return {
        'exchangeMaxTickets': safe_int(raw.get('exchangeMaxTickets', 100), 100),
        'exchangeExternalSell': g('exchangeExternalSell', 2.0),
        'exchangeExternalBuy': g('exchangeExternalBuy', 10.0),
        'exchangeExternalInstantSell': g('exchangeExternalInstantSell', 1.5),
        'exchangeExternalInstantBuy': g('exchangeExternalInstantBuy', raw.get('exchangeExternalIntantBuy', 12.0)),
        'exchangeAmountScaler': g('exchangeAmountScaler', 1.2),
        'exchangeAmountBuffer': g('exchangeAmountBuffer', 10.0),
        'cellCapacity': g('cellCapacity', 120.0),
        'cellChargeRate': g('cellChargeRate', 15.0),
        'cellDischargeRate': g('cellDischargeRate', 20.0),
        'corridorSun': g('corridorSun', 0.5),
        'corridorWind': g('corridorWind', 0.0),
        'corridorFactory': g('corridorFactory', 0.5),
        'corridorOffice': g('corridorOffice', 0.5),
        'corridorHospital': g('corridorHospital', 0.25),
        'corridorHouseA': g('corridorHouseA', 0.5),
        'corridorHouseB': g('corridorHouseB', 0.5),
        'maxSolarPower': g('maxSolarPower', 20.0),
        'maxWindPower': g('maxWindPower', 20.0),
        'exchangeConsumerPriceStep': g('exchangeConsumerPriceStep', 0.2),
        'weatherEffectsDelay': safe_int(raw.get('weatherEffectsDelay', 0), 0),
    }


# =====================
# Main controller
# =====================

def controller(psm: Any) -> Dict[str, Any]:
    state = load_state()
    tick = get_tick(psm)
    game_length = get_game_length(psm)
    cfg = normalize_cfg(get_config_dict(psm))
    object_rows = extract_object_rows(psm)
    network_rows = extract_network_rows(psm)
    exchange_rows = [exchange_receipt_data(x) for x in get_exchange_list(psm)]

    total_generated, total_consumed, total_external, total_losses = get_total_power_tuple(psm)
    net_agg = aggregate_networks(network_rows)
    obj_agg = aggregate_objects(object_rows)
    weather = {'wind': get_weather_now(psm, 'wind'), 'sun': get_weather_now(psm, 'sun')}
    forecast_bundle = get_forecast_bundle(psm)
    state['cfg_runtime'] = cfg
    state['weather_runtime'] = weather

    # Detailed analytics
    update_forecast_error_log(tick, state, object_rows, weather, forecast_bundle, total_consumed)
    store_current_forecast_for_next_tick(tick, state, forecast_bundle, object_rows)
    update_models(state, object_rows, weather, forecast_bundle, tick, cfg, total_consumed=total_consumed, total_losses=total_losses)

    future = forecast_window(state, object_rows, forecast_bundle, tick, game_length, LOOKAHEAD)
    current_theoretical, object_prediction_rows = current_theoretical_metrics(
        state, object_rows, weather, forecast_bundle, tick, cfg, total_consumed=total_consumed, total_losses=total_losses
    )
    useful_now = compute_useful_energy(total_generated, total_consumed, total_losses)

    market_stats = analyze_exchange(exchange_rows)
    avg_price = market_stats.get('sell_avg_contracted_price')
    fill_ratio_now = market_stats.get('sell_fill_ratio')
    if avg_price is not None:
        state['market_ref'] = 0.78 * safe_float(state.get('market_ref', 8.0), 8.0) + 0.22 * avg_price
    else:
        exch_log = get_exchange_log(psm)
        if tick > 0 and tick - 1 < len(exch_log):
            last_log_price = safe_float(exch_log[tick - 1], state.get('market_ref', 8.0))
            state['market_ref'] = 0.90 * safe_float(state.get('market_ref', 8.0), 8.0) + 0.10 * last_log_price

    if fill_ratio_now is not None:
        state['fill_ratio_ewma'] = 0.76 * safe_float(state.get('fill_ratio_ewma', 0.72), 0.72) + 0.24 * fill_ratio_now

    if state.get('last_sell_volume') is not None:
        err = useful_now - safe_float(state.get('last_sell_volume', 0.0), 0.0)
        state['abs_err_ewma'] = 0.84 * safe_float(state.get('abs_err_ewma', 1.5), 1.5) + 0.16 * abs(err)

    if total_generated > 1e-9:
        current_loss_ratio = clamp(total_losses / total_generated, 0.0, 0.8)
        state['loss_ratio_ewma'] = 0.88 * safe_float(state.get('loss_ratio_ewma', 0.10), 0.10) + 0.12 * current_loss_ratio

    charge_orders, discharge_orders, battery_dbg = storage_policy(
        cfg, obj_agg['storages'], useful_now, future,
        safe_float(state.get('fill_ratio_ewma', 0.72), 0.72),
        tick, game_length,
        loss_ratio=safe_float(state.get('loss_ratio_ewma', 0.10), 0.10),
    )

    exportable_now = max(0.0, useful_now)
    reserve = compute_reserve(state, future, object_rows, total_losses, exportable_now)
    offer_cap = compute_offer_cap(state, cfg, tick, exportable_now)
    battery_sell_bonus = battery_dbg.get('discharge_for_sell', 0.0)
    sell_volume = exportable_now - reserve - battery_dbg['charge_total'] + battery_sell_bonus
    sell_volume = clamp(sell_volume, 0.0, offer_cap)
    sell_volume = round_vol(sell_volume)

    ladder = build_ladder(
        sell_volume,
        safe_float(state.get('market_ref', 8.0), 8.0),
        safe_float(state.get('fill_ratio_ewma', 0.72), 0.72),
        safe_int(cfg['exchangeMaxTickets'], 100),
        cfg,
    )

    # Place orders only for live psm with orders API
    if hasattr(psm, 'orders'):
        for sid, amount in charge_orders:
            if amount > 0.0:
                psm.orders.charge(sid, amount)
        for sid, amount in discharge_orders:
            if amount > 0.0:
                psm.orders.discharge(sid, amount)
        for volume, price in ladder:
            psm.orders.sell(volume, price)

    forecast_meta = forecast_bundle.get('_meta', {})

    summary_row = {
        'tick': tick,
        'game_length': game_length,
        'score_delta': round(get_score_delta(psm), 6),
        'total_score': get_total_score(psm),
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
        'net_surplus_now': round(useful_now, 6),
        'exportable_now': round(exportable_now, 6),
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
        'market_ref': round(safe_float(state.get('market_ref', 8.0), 8.0), 6),
        'fill_ratio_ewma': round(safe_float(state.get('fill_ratio_ewma', 0.72), 0.72), 6),
        'instant_abs': round(safe_float(market_stats.get('instant_abs_total', 0.0), 0.0), 6),
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
        'type_totals': {
            typ: {
                'count': data['count'],
                'generated': round(data['generated'], 6),
                'consumed': round(data['consumed'], 6),
                'income': round(data['income'], 6),
                'loss': round(data['loss'], 6),
            }
            for typ, data in obj_agg['by_type'].items()
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
        },
        'models': state.get('object_models', {}),
        'future_window': future,
        'net_surplus_now': useful_now,
        'exportable_now': exportable_now,
        'reserve_formula': {
            'abs_err_term': 0.78 * safe_float(state.get('abs_err_ewma', 1.5), 1.5),
            'loss_term': 0.18 * total_losses,
            'generation_term': 0.04 * total_generated,
        },
        'offer_cap': offer_cap,
        'battery': battery_dbg,
        'charge_orders': charge_orders,
        'discharge_orders': discharge_orders,
        'sell_volume': sell_volume,
        'ladder': ladder,
        'exchange_analysis': {
            'avg_price': avg_price,
            'fill_ratio_now': fill_ratio_now,
            'fill_ratio_ewma': safe_float(state.get('fill_ratio_ewma', 0.72), 0.72),
            'market_ref': safe_float(state.get('market_ref', 8.0), 8.0),
            'instant_abs': safe_float(market_stats.get('instant_abs_total', 0.0), 0.0),
        },
    }

    cumulative = update_cumulative_state(
        state, obj_agg, total_generated, total_consumed, total_losses, useful_now, market_stats
    )

    derived_row = {
        'tick': tick,
        'sun_now': round(weather['sun'], 6),
        'forecast_source': forecast_meta.get('source'),
        'forecast_rows': forecast_meta.get('rows'),
        'wind_now': round(weather['wind'], 6),
        'solar_actual': round(safe_float(obj_agg['by_type'].get('solar', {}).get('generated', 0.0), 0.0), 6),
        'wind_actual': round(safe_float(obj_agg['by_type'].get('wind', {}).get('generated', 0.0), 0.0), 6),
        'gross_actual': round(total_generated, 6),
        'solar_theoretical_now': round(current_theoretical['solar_theoretical_now'], 6),
        'wind_theoretical_now': round(current_theoretical['wind_theoretical_now'], 6),
        'gross_theoretical_now': round(current_theoretical['gross_theoretical_now'], 6),
        'load_forecast_now': round(current_theoretical['load_forecast_now'], 6),
        'load_actual_now': round(total_consumed, 6),
        'useful_energy_now': round(useful_now, 6),
        'exportable_now': round(exportable_now, 6),
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
        'storage_charge_total': round(battery_dbg['charge_total'], 6),
        'storage_discharge_total': round(battery_dbg['discharge_total'], 6),
        'offer_cap': round(offer_cap, 6),
        'reserve': round(reserve, 6),
        'sell_volume': round(sell_volume, 6),
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

    log_tick_data(
        tick, object_rows, network_rows, exchange_rows, summary_row, strategy_row, derived_row, object_prediction_rows
    )

    state['prev_useful'] = useful_now
    state['prev_exportable'] = exportable_now
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
        err = {
            'tick': get_tick(psm),
            'error': str(e),
            'traceback': traceback.format_exc(),
        }
        write_jsonl(STRATEGY_DEBUG_FILE, err)
        print(json.dumps(err, ensure_ascii=False))
    psm.save_and_exit()


if __name__ == '__main__':
    main()
def current_theoretical_metrics(
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    weather: Dict[str, float],
    bundle: Dict[str, Dict[str, Any]],
    tick: int,
    cfg: Optional[Dict[str, Any]] = None,
    total_consumed: Optional[float] = None,
    total_losses: Optional[float] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    sun_now = weather['sun']
    wind_now = weather['wind']
    sun_spread = get_forecast_spread(bundle, 'sun', 0.0)
    wind_spread = get_forecast_spread(bundle, 'wind', 0.0)

    object_prediction_rows = []
    totals = {
        'solar_theoretical_now': 0.0,
        'wind_theoretical_now': 0.0,
        'gross_theoretical_now': 0.0,
        'load_forecast_now': aggregate_forecast_load(bundle, object_rows, tick),
        'load_model_now': 0.0,
        'loss_theoretical_now': 0.0,
    }

    counts = count_forecast_objects(object_rows)
    base_bias = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)

    for row in object_rows:
        typ = row['type']
        gen_theoretical = predict_object_generation(state, row, sun_now, wind_now, sun_spread, wind_spread, cfg=cfg, weather=weather)
        if typ == 'solar':
            totals['solar_theoretical_now'] += gen_theoretical
        elif typ == 'wind':
            totals['wind_theoretical_now'] += gen_theoretical
        totals['gross_theoretical_now'] += gen_theoretical

    totals['load_model_now'] = totals['load_forecast_now'] * base_bias
    totals['loss_theoretical_now'] = predict_total_losses(state, totals['gross_theoretical_now'], totals['load_model_now'])

    per_type_load_now = {}
    for typ, fc_name in OBJECT_TYPE_TO_FORECAST.items():
        cnt = counts.get(typ, 0)
        if cnt <= 0:
            continue
        fc_val = get_forecast_value(bundle, fc_name, tick)
        per_type_load_now[typ] = fc_val * base_bias

    for row in object_rows:
        typ = row['type']
        gen_theoretical = predict_object_generation(state, row, sun_now, wind_now, sun_spread, wind_spread, cfg=cfg, weather=weather)
        load_fc = 0.0
        load_model = 0.0
        load_key = OBJECT_TYPE_TO_FORECAST.get(typ)
        if load_key:
            load_fc = get_forecast_value(bundle, load_key, tick)
            load_model = per_type_load_now.get(typ, 0.0)

        gen_err = row['generated'] - gen_theoretical if typ in ('solar', 'wind') else 0.0
        load_err = row['consumed'] - load_model if load_key else 0.0
        object_prediction_rows.append({
            'tick': tick,
            'address': row['address'],
            'type': typ,
            'gen_actual': round(row['generated'], 6),
            'gen_theoretical_now': round(gen_theoretical, 6),
            'gen_model_error': round(gen_err, 6),
            'load_actual': round(row['consumed'], 6),
            'load_forecast_now': round(load_fc, 6),
            'load_model_now': round(load_model, 6),
            'load_model_error': round(load_err, 6),
            'sun_now': round(sun_now, 6),
            'wind_now': round(wind_now, 6),
        })

    return totals, object_prediction_rows



