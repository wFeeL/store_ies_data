import csv
import json
import os
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

import ips

GAME_TICKS = 100
FORECAST_DIR = os.path.expanduser('~/ips3-sandbox')
DEFAULT_FORECAST_FILE = os.path.join(FORECAST_DIR, 'forecast.csv')

MARKET_INSIGHTS = {
    'good_fill_ratio': 0.88,
    'weak_fill_ratio': 0.72,
    'bad_fill_ratio': 0.58,
    'preferred_ask_low': 2.8,
    'preferred_ask_high': 5.0,
    'overpriced_ask': 7.2,
}

# Softened empirical priors: still keep relative ordering from archive games,
# but stay closer to neutral because object estimates are not yet fully trusted.
PRODUCER_INSIGHTS = {
    's1': {'strength': 1.00, 'stability': 0.66, 'storm_risk': 0.00},
    's7': {'strength': 1.02, 'stability': 0.64, 'storm_risk': 0.00},
    's8': {'strength': 0.95, 'stability': 0.64, 'storm_risk': 0.00},
    'a3': {'strength': 0.96, 'stability': 0.66, 'storm_risk': 0.16},
    'a5': {'strength': 1.00, 'stability': 0.61, 'storm_risk': 0.38},
    'a6': {'strength': 0.98, 'stability': 0.63, 'storm_risk': 0.24},
    'a8': {'strength': 0.97, 'stability': 0.62, 'storm_risk': 0.22},
}

STATE_FILE = os.path.expanduser('~/ips3-sandbox/ies_state.json')
LOG_DIR = os.path.expanduser('~/ips3-sandbox/ies_logs')
TICK_SUMMARY_FILE = os.path.join(LOG_DIR, 'tick_summary.jsonl')
STRATEGY_DEBUG_FILE = os.path.join(LOG_DIR, 'strategy_debug.jsonl')

MIN_ORDER_VOLUME = 0.25
MIN_RESERVE = 0.8
MARKET_PRICE_MIN = 2.0
MARKET_PRICE_MAX = 20.0
SOC_FLOOR_FRAC = 0.06
SOC_CEIL_FRAC = 0.95
ENDGAME_TICKS = 5
LOOKAHEAD = max(8, GAME_TICKS // 12)
MARKET_HISTORY_WINDOW = 8
STRICT_FIRST_TICK_CAP = True

SOLAR_SEED_FACTORS = {
    's1': 0.50,
    's7': 0.62,
    's8': 0.47,
}
WIND_SEED_FACTORS = {
    'a3': 0.0046,
    'a5': 0.0050,
    'a6': 0.0048,
    'a8': 0.0047,
}
LOAD_BIAS_PRIOR = 0.60
LOAD_TYPE_BIAS_PRIORS = {
    'factory': 0.48,
    'houseA': 0.46,
    'houseB': 0.52,
    'office': 0.57,
    'hospital': 0.62,
}
LOAD_TYPE_BIAS_BOUNDS = {
    'factory': (0.26, 1.18),
    'houseA': (0.24, 1.18),
    'houseB': (0.26, 1.14),
    'office': (0.28, 1.18),
    'hospital': (0.36, 1.22),
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
    hi = clamp(float(price_max), MARKET_PRICE_MIN, MARKET_PRICE_MAX)
    x = clamp(float(x), MARKET_PRICE_MIN, hi)
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


def avg(values: List[float], default: float = 0.0) -> float:
    if not values:
        return float(default)
    return sum(float(v) for v in values) / float(len(values))


def addr_to_str(address: Any) -> str:
    if isinstance(address, list):
        return '|'.join(str(x) for x in address)
    if isinstance(address, tuple):
        return '|'.join(str(x) for x in address)
    return str(address)


_NONCRITICAL_IO_GUARD = False


def _report_noncritical_io_error(op: str, path: str, exc: Exception) -> None:
    global _NONCRITICAL_IO_GUARD
    payload = {
        'kind': 'noncritical_io_error',
        'op': op,
        'path': path,
        'error': f'{type(exc).__name__}: {exc}',
    }
    try:
        print(f"[noncritical-io] {op} {path}: {exc}", file=sys.stderr)
    except Exception:
        pass
    if _NONCRITICAL_IO_GUARD or os.path.abspath(path) == os.path.abspath(STRATEGY_DEBUG_FILE):
        return
    _NONCRITICAL_IO_GUARD = True
    try:
        os.makedirs(os.path.dirname(STRATEGY_DEBUG_FILE), exist_ok=True)
        with open(STRATEGY_DEBUG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(payload, ensure_ascii=False) + '\n')
    except Exception:
        pass
    finally:
        _NONCRITICAL_IO_GUARD = False


def ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as exc:
        _report_noncritical_io_error('mkdir', path, exc)


def write_jsonl(path: str, row: Dict[str, Any]) -> None:
    try:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    except Exception as exc:
        _report_noncritical_io_error('write_jsonl', path, exc)


def reset_runtime_outputs() -> None:
    ensure_dir(LOG_DIR)
    for path in [
        TICK_SUMMARY_FILE,
        STRATEGY_DEBUG_FILE,
    ]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as exc:
            _report_noncritical_io_error('remove', path, exc)


# =====================
# Forecast helpers
# =====================

def normalize_forecast_header(name: Any) -> str:
    s = str(name or '').strip().lower().replace('ё', 'е').replace('_', ' ').replace('-', ' ')
    s = ' '.join(s.split())
    return FORECAST_HEADER_TO_KEY.get(s, '')


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
        'series_lengths': {name: len(values) for name, values in series.items() if values},
        'bundle': {
            name: {'data': list(values), 'spread': None, 'source': 'csv'}
            for name, values in series.items()
            if values
        },
    }


def find_external_forecast_file() -> Optional[str]:
    preferred = [DEFAULT_FORECAST_FILE]
    for path in preferred:
        path = os.path.abspath(os.path.expanduser(path))
        if _parse_forecast_csv(path) is not None:
            return path
    try:
        candidates = []
        for entry in os.scandir(FORECAST_DIR):
            if not entry.is_file():
                continue
            if not entry.name.lower().startswith('forecast') or not entry.name.lower().endswith('.csv'):
                continue
            candidates.append((entry.stat().st_mtime_ns, entry.path))
        for _, path in sorted(candidates, reverse=True):
            if _parse_forecast_csv(path) is not None:
                return path
    except Exception:
        return None
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


def corridor_fallback_value(cfg: Optional[Dict[str, Any]], name: str) -> float:
    cfg = cfg or {}
    corridor_map = {
        'sun': 'corridorSun',
        'wind': 'corridorWind',
        'hospital': 'corridorHospital',
        'factory': 'corridorFactory',
        'office': 'corridorOffice',
        'houseA': 'corridorHouseA',
        'houseB': 'corridorHouseB',
    }
    default_map = {
        'sun': 0.5,
        'wind': 0.5,
        'hospital': 0.25,
        'factory': 0.5,
        'office': 0.5,
        'houseA': 0.5,
        'houseB': 0.5,
    }
    key = corridor_map.get(name)
    default = default_map.get(name, 0.5)
    return safe_float(cfg.get(key, default), default) if key else default


def forecast_required_series(object_rows: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    required = ['sun', 'wind']
    if not object_rows:
        return required
    for row in object_rows:
        fc_name = OBJECT_TYPE_TO_FORECAST.get(row.get('type'))
        if fc_name and fc_name not in required:
            required.append(fc_name)
    return required


def forecast_validated_rows(bundle: Dict[str, Dict[str, Any]], game_length: int) -> int:
    meta = bundle.get('_meta', {})
    validated = safe_int(meta.get('rows', 0), 0)
    if validated > 0:
        return validated
    required_series = meta.get('required_series', ['sun', 'wind'])
    lengths = [safe_int(meta.get('series_lengths', {}).get(name, 0), 0) for name in required_series]
    lengths = [length for length in lengths if length > 0]
    if lengths:
        return min(max(GAME_TICKS, game_length), min(lengths))
    return 0


def forecast_has_valid_tick(bundle: Dict[str, Dict[str, Any]], tick: int) -> bool:
    if tick < 0:
        return False
    meta = bundle.get('_meta', {})
    required_rows = safe_int(meta.get('required_rows', GAME_TICKS), GAME_TICKS)
    return tick < forecast_validated_rows(bundle, required_rows)


def _extract_forecast_series(item: Any) -> Tuple[List[float], Optional[float]]:
    if item is None:
        return [], None
    if isinstance(item, dict):
        if 'data' in item:
            return [safe_float(v, 0.0) for v in item.get('data', [])], safe_float(item.get('spread', None), None)
        if 'forecast' in item:
            forecast = item.get('forecast', {})
            return [safe_float(v, 0.0) for v in forecast.get('values', [])], safe_float(item.get('spread', None), None)
    if isinstance(item, (list, tuple)):
        spread = safe_float(getattr(item, 'spread', None), None)
        return [safe_float(v, 0.0) for v in item], spread
    try:
        spread = safe_float(getattr(item, 'spread', None), None)
        return [safe_float(v, 0.0) for v in item], spread
    except Exception:
        return [], None


def load_native_forecast_bundle(psm: Any) -> Dict[str, Dict[str, Any]]:
    raw = psm.get('forecasts') if isinstance(psm, dict) else getattr(psm, 'forecasts', None)
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            name = FORECAST_INDEX_TO_NAME.get(idx, f'f{idx}')
            data, spread = _extract_forecast_series(item)
            if data:
                out[name] = {'data': data, 'spread': spread, 'source': 'psm'}
    elif isinstance(raw, dict):
        for name in FORECAST_SERIES_ORDER:
            data, spread = _extract_forecast_series(raw.get(name))
            if data:
                out[name] = {'data': data, 'spread': spread, 'source': 'psm'}
    else:
        for name in FORECAST_SERIES_ORDER:
            seq = getattr(raw, name, None) if raw is not None else None
            data, spread = _extract_forecast_series(seq)
            if data:
                out[name] = {'data': data, 'spread': spread, 'source': 'psm'}
    out['_meta'] = {
        'source': 'psm',
        'path': None,
        'rows': max((len(v.get('data', [])) for v in out.values() if isinstance(v, dict) and 'data' in v), default=0),
        'series_lengths': {name: len(item.get('data', [])) for name, item in out.items() if isinstance(item, dict) and 'data' in item},
    }
    return out


def harmonize_forecast_bundle(
    external: Optional[Dict[str, Any]],
    native: Dict[str, Dict[str, Any]],
    required_rows: int,
    cfg: Optional[Dict[str, Any]] = None,
    object_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    cfg = cfg or {}
    required_rows = max(GAME_TICKS, safe_int(required_rows, GAME_TICKS))
    required_series = forecast_required_series(object_rows)
    warnings: List[str] = []
    fallbacks: List[str] = []
    series_lengths: Dict[str, int] = {}
    out: Dict[str, Dict[str, Any]] = {}

    ext_bundle = (external or {}).get('bundle', {})
    native_bundle = native or {}

    for name in FORECAST_SERIES_ORDER:
        ext_item = ext_bundle.get(name)
        native_item = native_bundle.get(name)
        data: List[float] = []
        if ext_item:
            data = list(ext_item.get('data', []))[:required_rows]
        source = 'csv' if ext_item else 'psm'
        if len(data) < required_rows and native_item:
            tail = list(native_item.get('data', []))[len(data):required_rows]
            if tail:
                data.extend(tail)
                fallbacks.append(f'{name}:tail_from_psm')
        spread = safe_float(ext_item.get('spread', None), None) if ext_item else None
        if spread is None and native_item:
            native_spread = safe_float(native_item.get('spread', None), None)
            if native_spread is not None:
                spread = native_spread
                if ext_item:
                    fallbacks.append(f'{name}:spread_from_psm')
        if spread is None:
            spread = corridor_fallback_value(cfg, name)
            fallbacks.append(f'{name}:spread_from_config')
        if data:
            out[name] = {'data': data, 'spread': spread, 'source': source}
            series_lengths[name] = len(data)
        elif name in required_series:
            warnings.append(f'missing_series:{name}')

    available_lengths = [series_lengths.get(name, 0) for name in required_series if series_lengths.get(name, 0) > 0]
    validated_rows = min(required_rows, min(available_lengths)) if available_lengths else 0
    for name in required_series:
        series_len = series_lengths.get(name, 0)
        if 0 < series_len < required_rows:
            warnings.append(f'short_series:{name}:{series_len}/{required_rows}')
    if validated_rows < required_rows:
        warnings.append(f'forecast_horizon_short:{validated_rows}/{required_rows}')

    source = 'csv' if external else 'psm'
    out['_meta'] = {
        'source': source,
        'path': external.get('path') if external else None,
        'rows': validated_rows,
        'required_rows': required_rows,
        'required_series': required_series,
        'series_lengths': series_lengths,
        'fallbacks': sorted(set(fallbacks)),
        'warnings': sorted(set(warnings)),
    }
    return out


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
        value = safe_int(psm.get('gameLength', GAME_TICKS), GAME_TICKS)
    else:
        value = safe_int(getattr(psm, 'gameLength', GAME_TICKS), GAME_TICKS)
    return value if value > 0 else GAME_TICKS


def get_raw_score_delta(psm: Any) -> Any:
    return psm.get('scoreDelta') if isinstance(psm, dict) else getattr(psm, 'scoreDelta', None)


def _extract_explicit_total_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    direct = safe_float(value, None)
    if direct is not None:
        return direct
    if isinstance(value, dict):
        for key in ('total_score', 'totalScore', 'score_total', 'scoreTotal', 'total'):
            if key in value:
                total = safe_float(value.get(key), None)
                if total is not None:
                    return total
        return None
    for key in ('total_score', 'totalScore', 'score_total', 'scoreTotal', 'total'):
        total = safe_float(getattr(value, key, None), None)
        if total is not None:
            return total
    return None


def parse_score_breakdown(raw: Any) -> Dict[str, Any]:
    result = {
        'income': 0.0,
        'loss': 0.0,
        'total_score': None,
        'score_delta': 0.0,
        'format': 'missing',
        'ambiguous': False,
    }
    if raw is None:
        return result
    if isinstance(raw, dict):
        income = safe_float(raw.get('income', None), None)
        loss = safe_float(raw.get('loss', None), None)
        if income is not None or loss is not None:
            result['format'] = 'object'
            result['income'] = safe_float(income, 0.0)
            result['loss'] = safe_float(loss, 0.0)
            result['total_score'] = _extract_explicit_total_score(raw)
            result['score_delta'] = result['income'] - result['loss']
            return result
    income = safe_float(getattr(raw, 'income', None), None)
    loss = safe_float(getattr(raw, 'loss', None), None)
    if income is not None or loss is not None:
        result['format'] = 'object'
        result['income'] = safe_float(income, 0.0)
        result['loss'] = safe_float(loss, 0.0)
        result['total_score'] = _extract_explicit_total_score(raw)
        result['score_delta'] = result['income'] - result['loss']
        return result
    if isinstance(raw, (list, tuple)):
        if len(raw) == 2:
            result['format'] = 'list_receipt'
            result['income'] = safe_float(raw[0], 0.0)
            result['loss'] = safe_float(raw[1], 0.0)
        else:
            result['format'] = 'list_ambiguous'
            result['ambiguous'] = True
            if raw:
                income = safe_float(raw[0], None)
                if income is not None:
                    result['income'] = income
            if len(raw) > 1:
                loss = safe_float(raw[1], None)
                if loss is not None:
                    result['loss'] = loss
        result['score_delta'] = result['income'] - result['loss']
        return result
    delta = safe_float(raw, None)
    if delta is not None:
        result['format'] = 'scalar'
        result['income'] = delta
        result['score_delta'] = delta
        return result
    result['format'] = type(raw).__name__
    result['ambiguous'] = True
    return result


def _extract_total_score_from_psm(psm: Any) -> Optional[float]:
    keys = ('total_score', 'totalScore', 'score_total', 'scoreTotal')
    if isinstance(psm, dict):
        for key in keys:
            if key in psm:
                total = _extract_explicit_total_score(psm.get(key))
                if total is not None:
                    return total
        return None
    for key in keys:
        total = _extract_explicit_total_score(getattr(psm, key, None))
        if total is not None:
            return total
    return None


def get_score_breakdown(psm: Any) -> Tuple[float, float, Optional[float]]:
    parsed = parse_score_breakdown(get_raw_score_delta(psm))
    total_score = parsed.get('total_score')
    if total_score is None:
        total_score = _extract_total_score_from_psm(psm)
    return safe_float(parsed.get('income', 0.0), 0.0), safe_float(parsed.get('loss', 0.0), 0.0), safe_float(total_score, None)


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


def get_forecast_bundle(
    psm: Any,
    game_length: int = GAME_TICKS,
    cfg: Optional[Dict[str, Any]] = None,
    object_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    ext = load_external_forecast_csv()
    native = load_native_forecast_bundle(psm)
    return harmonize_forecast_bundle(ext, native, max(GAME_TICKS, game_length), cfg=cfg, object_rows=object_rows)


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
        'prev_useful_energy_actual': None,
        'last_sell_volume': 0.0,
        'abs_err_ewma': 1.2,
        'loss_ratio_ewma': 0.18,
        'fill_ratio_ewma': 0.84,
        'market_ref': 4.8,
        'market_history': [],
        'load_bias_total': LOAD_BIAS_PRIOR,
        'load_abs_err': 2.0,
        'startup_load_scale': 1.0,
        'startup_mode_until': -1,
        'startup_last_ratio': 1.0,
        'startup_last_update_tick': -1,
        'load_mix': {'counts': {}, 'houseb_share': 0.0},
        'object_models': {},
        'storage_mode': 'hold',
        'storage_mode_lock_until': -1,
        'weather_history': {'wind': [], 'sun': []},
        'loss_model': dict(LOSS_MODEL_PRIOR, scale=1.0),
    }


def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.pop('forecast_profile', None)
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
    except Exception as exc:
        _report_noncritical_io_error('save_state', STATE_FILE, exc)


def _model_key(address: str, kind: str) -> str:
    return f'{kind}:{address}'


def get_model(state: Dict[str, Any], key: str, kind: str) -> Dict[str, Any]:
    models = state.setdefault('object_models', {})
    mkey = _model_key(key, kind)
    if mkey not in models:
        if kind == 'solar':
            prior = PRODUCER_INSIGHTS.get(key, {})
            models[mkey] = {
                'kind': kind,
                'factor': SOLAR_SEED_FACTORS.get(key, 0.65),
                'err': 0.8,
                'samples': 0,
                'strength_bias': safe_float(prior.get('strength', 1.0), 1.0),
            }
        elif kind == 'wind':
            prior = PRODUCER_INSIGHTS.get(key, {})
            models[mkey] = {
                'kind': kind,
                'factor': WIND_SEED_FACTORS.get(key, 0.0050),
                'rot_factor': 80.0,
                'wind_to_rot': 0.040,
                'rot_curve': {},
                'max_power_seen': 0.0,
                'err': 2.5,
                'last_failed': 0,
                'samples': 0,
                'storm_risk': safe_float(prior.get('storm_risk', 0.0), 0.0),
                'strength_bias': safe_float(prior.get('strength', 1.0), 1.0),
            }
        else:
            models[mkey] = {'kind': kind, 'bias': None, 'err': 0.6, 'samples': 0}
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
    if not forecast_has_valid_tick(bundle, tick):
        return 0.0
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


def get_type_load_prior(state: Dict[str, Any], obj_type: str) -> float:
    prior = safe_float(LOAD_TYPE_BIAS_PRIORS.get(obj_type, LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
    mix = state.get('load_mix', {})
    houseb_share = safe_float(mix.get('houseb_share', 0.0), 0.0)
    if obj_type == 'houseB' and houseb_share > 0.25:
        damp = clamp(1.0 - 0.55 * (houseb_share - 0.25), 0.74, 1.0)
        prior *= damp
    return clamp(prior, 0.20, 1.20)


def get_type_load_bounds(state: Dict[str, Any], obj_type: str) -> Tuple[float, float]:
    lo, hi = LOAD_TYPE_BIAS_BOUNDS.get(obj_type, (0.20, 1.20))
    mix = state.get('load_mix', {})
    houseb_share = safe_float(mix.get('houseb_share', 0.0), 0.0)
    if obj_type == 'houseB' and houseb_share > 0.25:
        over = clamp((houseb_share - 0.25) / 0.35, 0.0, 1.0)
        hi = min(hi, 1.12 - 0.08 * over)
    return clamp(lo, 0.10, 2.00), clamp(max(lo, hi), lo, 2.00)


def clamp_storage_soc(value: Any, cell_capacity: float) -> float:
    return clamp(safe_float(value, 0.0), 0.0, max(0.0, cell_capacity))


def startup_active(state: Dict[str, Any], tick: int) -> bool:
    return 0 <= safe_int(state.get('startup_mode_until', -1), -1) and tick <= safe_int(state.get('startup_mode_until', -1), -1)


def startup_scale(state: Dict[str, Any], tick: int) -> float:
    scale = clamp(safe_float(state.get('startup_load_scale', 1.0), 1.0), 0.0, 1.0)
    return scale if startup_active(state, tick) or scale < 0.999 else 1.0


def startup_bias_active(state: Dict[str, Any], tick: int) -> bool:
    return startup_active(state, tick) or startup_scale(state, tick) < 0.995


def blended_load_base_bias(state: Dict[str, Any], obj_type: str, tick: int, type_prior: Optional[float] = None) -> float:
    prior = get_type_load_prior(state, obj_type) if type_prior is None else safe_float(type_prior, get_type_load_prior(state, obj_type))
    total_bias = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR)
    prior_weight = 0.45
    scale = startup_scale(state, tick)
    if scale < 0.999:
        prior_weight *= 0.25 + 0.75 * scale
    return clamp((1.0 - prior_weight) * total_bias + prior_weight * prior, 0.02, 1.20)


def effective_load_trust(state: Dict[str, Any], model: Dict[str, Any], obj_type: str, tick: int) -> float:
    samples = safe_int(model.get('samples', 0), 0)
    trust = clamp(0.10 + 0.08 * samples, 0.14, 0.72)
    if obj_type == 'houseB' and safe_float(state.get('load_mix', {}).get('houseb_share', 0.0), 0.0) > 0.35:
        trust = min(trust, 0.48)
    if model.get('bias') is None:
        trust = min(trust, 0.18)
    scale = startup_scale(state, tick)
    if scale < 0.999 and model.get('bias') is not None:
        trust = max(trust, 0.24 + 0.26 * (1.0 - scale))
    return clamp(trust, 0.14, 0.72)


def effective_load_bounds(state: Dict[str, Any], obj_type: str, tick: int) -> Tuple[float, float]:
    lo, hi = get_type_load_bounds(state, obj_type)
    scale = startup_scale(state, tick)
    if scale < 0.999:
        lo = min(lo, 0.02 + 0.18 * scale)
    return lo, hi


def refresh_static_runtime_context(state: Dict[str, Any], object_rows: List[Dict[str, Any]]) -> None:
    load_counts = count_forecast_objects(object_rows)
    total_load_objects = max(1, sum(load_counts.values()))
    state['load_mix'] = {
        'counts': load_counts,
        'total_objects': total_load_objects,
        'houseb_share': load_counts.get('houseB', 0) / float(total_load_objects),
    }


def apply_startup_observation(
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    bundle: Dict[str, Dict[str, Any]],
    tick: int,
    total_consumed: Optional[float] = None,
) -> None:
    if total_consumed is None:
        return
    if safe_int(state.get('startup_last_update_tick', -1), -1) == tick:
        return

    total_fc_now = aggregate_forecast_load(bundle, object_rows, tick)
    prev_scale = clamp(safe_float(state.get('startup_load_scale', 1.0), 1.0), 0.0, 1.0)

    if total_fc_now > 1e-6:
        observed_ratio = clamp(total_consumed / max(total_fc_now, 1e-6), 0.0, 1.10)
        state['startup_last_ratio'] = observed_ratio
        if tick == 0 and observed_ratio < 0.15:
            state['startup_mode_until'] = max(safe_int(state.get('startup_mode_until', -1), -1), 4)
        elif tick == 1 and observed_ratio < 0.30:
            state['startup_mode_until'] = max(safe_int(state.get('startup_mode_until', -1), -1), 4)

        startup_now = startup_active(state, tick)
        if startup_now:
            scale_target = clamp(observed_ratio, 0.0, 1.0)
            if tick <= 1 and observed_ratio < 0.35:
                scale = clamp(0.18 * prev_scale + 0.82 * scale_target, 0.0, 1.0)
                startup_cap = clamp(0.14 + 0.45 * observed_ratio, 0.14, 0.45)
                state['startup_load_scale'] = min(scale, startup_cap)
            else:
                scale = clamp(0.35 * prev_scale + 0.65 * scale_target, 0.0, 1.0)
                state['startup_load_scale'] = min(scale, prev_scale + 0.25)
            if observed_ratio > 0.75:
                state['startup_mode_until'] = tick - 1
        elif prev_scale < 0.999:
            scale_target = clamp(observed_ratio, 0.0, 1.0)
            scale_alpha = 0.28 if scale_target >= prev_scale else 0.38
            state['startup_load_scale'] = clamp((1.0 - scale_alpha) * prev_scale + scale_alpha * scale_target, 0.0, 1.0)

        startup_bias_now = startup_bias_active(state, tick)
        load_bias = total_consumed / max(total_fc_now, 1e-6)
        load_bias = clamp(load_bias, 0.02 if startup_bias_now else 0.28, 1.10)
        alpha = 0.35 if startup_bias_now else 0.10
        state['load_bias_total'] = (1.0 - alpha) * safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) + alpha * load_bias
        pred_total = safe_float(state.get('load_bias_total', LOAD_BIAS_PRIOR), LOAD_BIAS_PRIOR) * total_fc_now
        state['load_abs_err'] = 0.90 * safe_float(state.get('load_abs_err', 2.0), 2.0) + 0.10 * abs(total_consumed - pred_total)
    elif prev_scale < 0.999 and tick > safe_int(state.get('startup_mode_until', -1), -1):
        state['startup_load_scale'] = min(1.0, clamp(prev_scale + 0.18, 0.0, 1.0))

    state['startup_last_update_tick'] = tick


def apply_post_tick_learning(
    state: Dict[str, Any],
    object_rows: List[Dict[str, Any]],
    weather: Dict[str, float],
    bundle: Dict[str, Dict[str, Any]],
    tick: int,
    cfg: Optional[Dict[str, Any]] = None,
    total_consumed: Optional[float] = None,
    total_losses: Optional[float] = None,
    marketable_useful_now: Optional[float] = None,
    total_generated: Optional[float] = None,
) -> None:
    sun_now = max(0.0, weather['sun'])
    wind_now = max(0.0, weather['wind'])
    hist = state.setdefault('weather_history', {'wind': [], 'sun': []})
    hist['wind'] = (hist.get('wind') or [])[-12:] + [wind_now]
    hist['sun'] = (hist.get('sun') or [])[-12:] + [sun_now]
    apply_startup_observation(
        state,
        object_rows,
        bundle,
        tick,
        total_consumed=total_consumed,
    )

    if total_consumed is not None and total_losses is not None:
        pred_loss = predict_total_losses(state, sum(r['generated'] for r in object_rows), total_consumed)
        if pred_loss > 1e-6:
            scale = total_losses / pred_loss
            lm = state.setdefault('loss_model', dict(LOSS_MODEL_PRIOR))
            lm['scale'] = 0.88 * safe_float(lm.get('scale', 1.0), 1.0) + 0.12 * clamp(scale, 0.55, 1.8)
    if total_generated is not None and total_losses is not None and total_generated > 1e-9:
        current_loss_ratio = clamp(total_losses / total_generated, 0.0, 0.8)
        state['loss_ratio_ewma'] = 0.88 * safe_float(state.get('loss_ratio_ewma', 0.18), 0.18) + 0.12 * current_loss_ratio
    if marketable_useful_now is not None:
        prev_useful_est = state.get('prev_useful_supply_est')
        if prev_useful_est is not None:
            err = marketable_useful_now - safe_float(prev_useful_est, 0.0)
            state['abs_err_ewma'] = 0.84 * safe_float(state.get('abs_err_ewma', 1.2), 1.2) + 0.16 * abs(err)

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
            model['samples'] = safe_int(model.get('samples', 0), 0) + 1
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
            model['samples'] = safe_int(model.get('samples', 0), 0) + 1
        elif typ in OBJECT_TYPE_TO_FORECAST:
            model = get_model(state, key, 'load')
            actual = row['consumed']
            fc_name = OBJECT_TYPE_TO_FORECAST.get(typ)
            fc_now = get_forecast_value(bundle, fc_name, tick)
            type_prior = get_type_load_prior(state, typ)
            lo, hi = effective_load_bounds(state, typ, tick)
            base_bias = clamp(blended_load_base_bias(state, typ, tick, type_prior=type_prior), lo, hi)
            model_bias = safe_float(model.get('bias', base_bias), base_bias)
            adapt = 0.08
            scale = startup_scale(state, tick)
            if scale < 0.999:
                adapt = clamp(0.22 + 0.45 * (1.0 - scale), 0.22, 0.65)
            if fc_now > 0.05 and actual >= 0.0:
                est_bias = actual / max(fc_now, 1e-6)
                target_bias = clamp(est_bias, lo, hi)
                model['bias'] = (1.0 - adapt) * model_bias + adapt * target_bias
            else:
                model['bias'] = 0.97 * model_bias + 0.03 * base_bias
            pred = clamp(safe_float(model.get('bias', base_bias), base_bias), lo, hi) * max(fc_now, 0.0)
            model['err'] = 0.92 * safe_float(model.get('err', 0.6), 0.6) + 0.08 * abs(actual - pred)
            model['samples'] = safe_int(model.get('samples', 0), 0) + 1


def update_models(state: Dict[str, Any], object_rows: List[Dict[str, Any]], weather: Dict[str, float], bundle: Dict[str, Dict[str, Any]], tick: int, cfg: Optional[Dict[str, Any]] = None, total_consumed: Optional[float] = None, total_losses: Optional[float] = None) -> None:
    refresh_static_runtime_context(state, object_rows)
    apply_post_tick_learning(
        state,
        object_rows,
        weather,
        bundle,
        tick,
        cfg=cfg,
        total_consumed=total_consumed,
        total_losses=total_losses,
    )


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


def update_market_history(state: Dict[str, Any], tick: int, market_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    history = list(state.get('market_history', []))
    history.append({
        'tick': tick,
        'sell_asked': safe_float(market_stats.get('sell_asked', 0.0), 0.0),
        'sell_contracted': safe_float(market_stats.get('sell_contracted', 0.0), 0.0),
        'fill_ratio': safe_float(market_stats.get('sell_fill_ratio', 0.0), 0.0),
        'avg_ask_price': safe_float(market_stats.get('sell_avg_asked_price', 0.0), 0.0),
        'avg_contracted_price': safe_float(market_stats.get('sell_avg_contracted_price', 0.0), 0.0),
    })
    history = history[-MARKET_HISTORY_WINDOW:]
    state['market_history'] = history
    return history


def build_market_context(state: Dict[str, Any], market_stats: Dict[str, Any]) -> Dict[str, Any]:
    history = list(state.get('market_history', []))
    fill_samples = [safe_float(row.get('fill_ratio', 0.0), 0.0) for row in history if safe_float(row.get('sell_asked', 0.0), 0.0) > 0.25]
    ask_samples = [safe_float(row.get('avg_ask_price', 0.0), 0.0) for row in history if safe_float(row.get('sell_asked', 0.0), 0.0) > 0.25]
    has_fill_history = len(fill_samples) >= 2
    has_ask_history = len(ask_samples) >= 2
    weak_fill_ratio = safe_float(MARKET_INSIGHTS.get('weak_fill_ratio', 0.78), 0.78)
    good_fill_ratio = safe_float(MARKET_INSIGHTS.get('good_fill_ratio', 0.92), 0.92)
    overpriced_ask = safe_float(MARKET_INSIGHTS.get('overpriced_ask', 6.2), 6.2)
    preferred_ask_high = safe_float(MARKET_INSIGHTS.get('preferred_ask_high', 4.6), 4.6)
    neutral_fill = clamp(
        max(safe_float(state.get('fill_ratio_ewma', 0.84), 0.84), weak_fill_ratio + 0.06),
        weak_fill_ratio + 0.06,
        good_fill_ratio,
    )
    recent_fill = avg(fill_samples, default=neutral_fill)
    recent_ask = avg(ask_samples, default=safe_float(state.get('market_ref', 4.8), 4.8))
    price_realism = 1.0
    if has_ask_history and recent_ask > preferred_ask_high:
        over = recent_ask - preferred_ask_high
        span = max(0.8, overpriced_ask - preferred_ask_high)
        price_realism = clamp(1.0 - over / span, 0.18, 1.0)
    if has_fill_history and recent_fill < weak_fill_ratio:
        price_realism *= clamp(0.65 + 0.55 * recent_fill / max(weak_fill_ratio, 1e-6), 0.22, 1.0)
    overpriced = has_ask_history and has_fill_history and recent_ask >= overpriced_ask and recent_fill < weak_fill_ratio
    return {
        'history': history,
        'recent_fill_ratio': recent_fill,
        'recent_ask_price': recent_ask,
        'price_realism': price_realism,
        'has_fill_history': has_fill_history,
        'good_fill': has_fill_history and recent_fill >= good_fill_ratio,
        'weak_fill': has_fill_history and recent_fill < weak_fill_ratio,
        'overpriced': overpriced,
    }


def compute_useful_energy(total_generated: float, total_consumed: float, total_losses: float) -> float:
    return total_generated - total_losses


def compute_balance_energy(total_generated: float, total_consumed: float, total_losses: float) -> float:
    return total_generated - total_consumed - total_losses


def compute_offer_cap(state: Dict[str, Any], cfg: Dict[str, Any], tick: int, useful_supply_now: float) -> float:
    prev_useful = state.get('prev_useful_energy_actual')
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
        pred *= clamp(safe_float(model.get('strength_bias', 1.0), 1.0), 0.75, 1.08)
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
        pred *= clamp(safe_float(model.get('strength_bias', 1.0), 1.0), 0.72, 1.10)
        pred *= clamp(1.0 - 0.05 * safe_float(model.get('err', 2.5), 2.5), 0.70, 1.00)
        failed_now = safe_int(row.get('failed', 0), 0)
        if failed_now > 0:
            pred *= 0.60
        elif safe_int(model.get('last_failed', 0), 0) > 0:
            pred *= 0.82
        wind_limit = safe_float(cfg.get('weatherMaxWind', 15.0), 15.0)
        storm_risk = safe_float(model.get('storm_risk', 0.0), 0.0)
        if eff_wind > wind_limit * 0.85:
            pred *= clamp(0.92 - 0.12 * storm_risk, 0.72, 0.94)
        return clamp(pred, 0.0, max_wind_power)
    return 0.0


def predict_object_load(state: Dict[str, Any], row: Dict[str, Any], forecast_value: float, tick: int) -> float:
    key = row['address']
    typ = row['type']
    if typ not in OBJECT_TYPE_TO_FORECAST:
        return 0.0
    model = get_model(state, key, 'load')
    type_prior = get_type_load_prior(state, typ)
    lo, hi = effective_load_bounds(state, typ, tick)
    base_bias = clamp(blended_load_base_bias(state, typ, tick, type_prior=type_prior), lo, hi)
    model_bias = safe_float(model.get('bias', base_bias), base_bias)
    model_bias = clamp(model_bias, lo, hi)
    trust = effective_load_trust(state, model, typ, tick)
    bias = trust * model_bias + (1.0 - trust) * base_bias
    return max(0.0, forecast_value * clamp(bias, lo, hi))


def forecast_window(state: Dict[str, Any], object_rows: List[Dict[str, Any]], bundle: Dict[str, Dict[str, Any]], tick: int, game_length: int, horizon: int) -> List[Dict[str, Any]]:
    runtime_cfg = state.get('cfg_runtime', {})
    wind_spread = max(get_forecast_spread(bundle, 'wind', 0.0), safe_float(runtime_cfg.get('corridorWind', 0.5), 0.5))
    sun_spread = max(get_forecast_spread(bundle, 'sun', 0.0), safe_float(runtime_cfg.get('corridorSun', 0.5), 0.5))
    weather = state.get('weather_runtime', {})
    out: List[Dict[str, Any]] = []
    validated_rows = forecast_validated_rows(bundle, game_length)
    if validated_rows <= 0:
        return out
    last_tick = max(0, min(game_length, validated_rows) - 1)
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
            load_pred = predict_object_load(state, row, get_forecast_value(bundle, fc_name, t), t)
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


def build_forecast_profile(state: Dict[str, Any], bundle: Dict[str, Dict[str, Any]], object_rows: List[Dict[str, Any]], game_length: int) -> Dict[str, Any]:
    rows = min(game_length, forecast_validated_rows(bundle, game_length))
    if rows <= 0:
        return {'rows': 0, 'ticks': [], 'windows': {}}
    sun = [get_forecast_value(bundle, 'sun', t) for t in range(rows)]
    wind = [get_forecast_value(bundle, 'wind', t) for t in range(rows)]
    load = []
    for t in range(rows):
        total = 0.0
        for row in object_rows:
            fc_name = OBJECT_TYPE_TO_FORECAST.get(row.get('type'))
            if fc_name:
                total += predict_object_load(state, row, get_forecast_value(bundle, fc_name, t), t)
        load.append(total)

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
    return {'rows': rows, 'ticks': ticks, 'windows': windows}


def forecast_profile_context(profile: Dict[str, Any], tick: int, horizon: int = 12) -> Dict[str, Any]:
    ticks = profile.get('ticks', []) if isinstance(profile, dict) else []
    if not ticks:
        return {
            'current': {},
            'avg_combo_6': 0.0,
            'avg_combo_12': 0.0,
            'avg_risk_6': 0.0,
            'avg_risk_12': 0.0,
            'next_mixed_in': None,
            'next_risk_in': None,
            'next_solar_in': None,
        }
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


def compute_target_soc(cfg: Dict[str, Any], total_capacity: float, total_soc: float, future: List[Dict[str, Any]], fill_ratio: float, tick: int, game_length: int, loss_ratio: float = 0.10, profile_ctx: Optional[Dict[str, Any]] = None, market_ctx: Optional[Dict[str, Any]] = None) -> float:
    base_ceil = min(total_capacity, total_capacity * SOC_CEIL_FRAC)
    weighted_gap = 0.0
    weighted_surplus = 0.0
    raw_gap_sum = 0.0
    raw_surplus_sum = 0.0
    useful_gap_sum = 0.0
    max_gap = 0.0
    solar_preds: List[float] = []
    wind_preds: List[float] = []
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
        type_totals = row.get('type_totals', {}) if isinstance(row.get('type_totals', {}), dict) else {}
        solar_preds.append(safe_float(type_totals.get('solar', {}).get('gen', 0.0), 0.0))
        wind_preds.append(safe_float(type_totals.get('wind', {}).get('gen', 0.0), 0.0))
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
    solar_now = solar_preds[0] if solar_preds else 0.0
    solar_later = avg(solar_preds[1:4], default=solar_now)
    solar_drop_risk = max(0.0, solar_now - solar_later)
    wind_peak = max(wind_preds[:4], default=0.0)
    if solar_drop_risk > 0.8:
        target += min(0.18 * total_capacity, 0.16 * solar_drop_risk)
    if wind_peak > 12.0:
        target += min(0.14 * total_capacity, 0.02 * wind_peak * total_capacity / max(total_capacity, 1.0))
    if market_ctx:
        price_realism = safe_float(market_ctx.get('price_realism', 1.0), 1.0)
        recent_fill = safe_float(market_ctx.get('recent_fill_ratio', fill_ratio), fill_ratio)
        weak_fill_ratio = safe_float(MARKET_INSIGHTS.get('weak_fill_ratio', 0.78), 0.78)
        has_fill_history = bool(market_ctx.get('has_fill_history', False))
        weak_market_fill = has_fill_history and recent_fill < weak_fill_ratio
        anti_dump_headroom = safe_float(market_ctx.get('anti_dump_headroom', 0.0), 0.0)
        if weak_market_fill:
            target += 0.05 * total_capacity
        if price_realism < 0.60:
            target += 0.04 * total_capacity
        if anti_dump_headroom < 2.0 and raw_surplus_sum > 0.0:
            target += 0.03 * total_capacity
    target += 0.12 * max_gap
    ticks_left = max(0, game_length - tick)
    if ticks_left <= ENDGAME_TICKS:
        floor = 0.0
        target = max(0.0, 0.10 * weighted_gap)
    return clamp(target, floor, base_ceil)


def storage_policy(state: Dict[str, Any], cfg: Dict[str, Any], storages: List[Dict[str, Any]], balance_now: float, useful_now: float, future: List[Dict[str, Any]], fill_ratio: float, tick: int, game_length: int, loss_ratio: float = 0.10, profile_ctx: Optional[Dict[str, Any]] = None, market_ctx: Optional[Dict[str, Any]] = None) -> Tuple[List[Tuple[str, float]], List[Tuple[str, float]], Dict[str, Any]]:
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
            'soc_band': 0.0,
            'emergency_floor_soc': 0.0,
            'working_floor_soc': 0.0,
            'high_risk_target_soc': 0.0,
            'allow_market_discharge': False,
            'mode': 'hold',
            'signal': 0.0,
            'protected_soc': 0.0,
        }
    cell_capacity = safe_float(cfg['cellCapacity'], 120.0)
    charge_rate = safe_float(cfg['cellChargeRate'], 15.0)
    discharge_rate = safe_float(cfg['cellDischargeRate'], 20.0)
    norm_storages = [
        {'id': s['id'], 'soc': clamp_storage_soc(s.get('soc', 0.0), cell_capacity)}
        for s in storages
    ]
    total_capacity = len(norm_storages) * cell_capacity
    total_soc = sum(s['soc'] for s in norm_storages)
    total_charge_rate = len(norm_storages) * charge_rate
    total_discharge_rate = len(norm_storages) * discharge_rate
    soc_ceil = min(total_capacity, total_capacity * SOC_CEIL_FRAC)
    target_soc = compute_target_soc(cfg, total_capacity, total_soc, future, fill_ratio, tick, game_length, loss_ratio=loss_ratio, profile_ctx=profile_ctx, market_ctx=market_ctx)
    next_balance = safe_float(future[0].get('balance_pred', 0.0), 0.0) if future else 0.0
    next2_balance = safe_float(future[1].get('balance_pred', next_balance), next_balance) if len(future) > 1 else next_balance
    signal = 0.55 * balance_now + 0.30 * next_balance + 0.15 * next2_balance
    deficit_sum = sum(max(0.0, -safe_float(r.get('balance_pred', 0.0), 0.0)) for r in future)
    surplus_sum = sum(max(0.0, safe_float(r.get('balance_pred', 0.0), 0.0)) for r in future)
    next_deficit = max(0.0, -safe_float(future[0].get('balance_pred', 0.0), 0.0)) if future else 0.0
    chronic_deficit = deficit_sum > max(4.0, 1.5 * surplus_sum)
    floor_frac = 0.06 if chronic_deficit else SOC_FLOOR_FRAC
    if tick >= game_length - ENDGAME_TICKS:
        floor_frac = 0.0
    current_profile = profile_ctx.get('current', {}) if profile_ctx else {}
    next_risk_in = profile_ctx.get('next_risk_in') if profile_ctx else None
    next_mixed_in = profile_ctx.get('next_mixed_in') if profile_ctx else None
    severe_risk = bool(
        chronic_deficit and (
            loss_ratio > 0.38
            or current_profile.get('protect_bias', 0.0) > 0.5
            or (next_risk_in is not None and 0 <= next_risk_in <= 12)
        )
    )
    if next_risk_in is not None and 0 <= next_risk_in <= 18:
        floor_frac = max(floor_frac, 0.10)
    floor_soc = total_capacity * floor_frac
    recent_fill = safe_float(market_ctx.get('recent_fill_ratio', fill_ratio), fill_ratio) if market_ctx else fill_ratio
    price_realism = safe_float(market_ctx.get('price_realism', 1.0), 1.0) if market_ctx else 1.0
    overpriced_market = bool(market_ctx.get('overpriced', False)) if market_ctx else False
    has_fill_history = bool(market_ctx.get('has_fill_history', False)) if market_ctx else False
    weak_fill_ratio = safe_float(MARKET_INSIGHTS.get('weak_fill_ratio', 0.78), 0.78)
    good_fill_ratio = safe_float(MARKET_INSIGHTS.get('good_fill_ratio', 0.92), 0.92)
    weak_market_fill = has_fill_history and recent_fill < weak_fill_ratio
    market_sell_support = recent_fill >= good_fill_ratio if has_fill_history else price_realism >= 0.82
    anti_dump_headroom = safe_float(market_ctx.get('anti_dump_headroom', 0.0), 0.0) if market_ctx else 0.0
    emergency_floor_soc = max(floor_soc, total_capacity * (0.12 if severe_risk else 0.08 if chronic_deficit else 0.05))
    working_floor_soc = max(emergency_floor_soc, total_capacity * (0.18 if weak_market_fill or price_realism < 0.65 else 0.12))
    prep_soc = target_soc
    if next_risk_in is not None and 0 <= next_risk_in <= 18:
        prep_soc = max(prep_soc, (0.68 - 0.016 * min(next_risk_in, 18)) * total_capacity)
    elif current_profile.get('mixed_peak') or current_profile.get('solar_active'):
        prep_soc = max(prep_soc, 0.42 * total_capacity)
    if next_mixed_in is not None and 0 <= next_mixed_in <= 8 and next_risk_in is not None and 0 <= next_risk_in <= 18:
        prep_soc = max(prep_soc, 0.58 * total_capacity)
    if weak_market_fill or overpriced_market or anti_dump_headroom < 2.0:
        prep_soc = max(prep_soc, working_floor_soc + 0.08 * total_capacity)
    prep_soc = clamp(prep_soc, floor_soc, soc_ceil)
    high_risk_target_soc = max(prep_soc, total_capacity * (0.62 if severe_risk else 0.0))
    protected_soc = max(working_floor_soc, 0.88 * prep_soc if severe_risk else (0.80 * prep_soc if chronic_deficit else 0.72 * prep_soc))
    charge_total = 0.0
    discharge_total = 0.0
    discharge_for_market = 0.0
    current_deficit = max(0.0, -balance_now)
    current_surplus = max(0.0, balance_now)
    charge_room = max(0.0, total_capacity * SOC_CEIL_FRAC - total_soc)
    charge_cap = max(0.0, prep_soc + 0.02 * total_capacity - total_soc)
    discharge_floor_soc = clamp(
        max(protected_soc + 0.02 * total_capacity, prep_soc - 0.02 * total_capacity),
        floor_soc,
        soc_ceil,
    )
    discharge_cap = max(0.0, total_soc - discharge_floor_soc)
    market_soc_floor = max(protected_soc + 0.02 * total_capacity, prep_soc + (0.04 if tick >= game_length - 2 else 0.06) * total_capacity)
    prev_mode = str(state.get('storage_mode', 'hold'))
    locked = tick <= safe_int(state.get('storage_mode_lock_until', -1), -1)
    force_discharge = balance_now < -4.0 and total_soc > discharge_floor_soc + 0.08 * total_capacity
    force_charge = total_soc < floor_soc + 0.06 * total_capacity and balance_now >= 0.0
    desired_mode = 'hold'
    if force_discharge:
        desired_mode = 'discharge'
    elif force_charge:
        desired_mode = 'charge'
    elif total_soc < prep_soc - 0.05 * total_capacity and (signal > 2.0 or (next_risk_in is not None and next_risk_in <= 12 and next_balance > 1.0)):
        desired_mode = 'charge'
    elif total_soc > discharge_floor_soc + 0.05 * total_capacity and signal < -2.0:
        desired_mode = 'discharge'
    mode = prev_mode if locked and not (force_discharge or force_charge) else desired_mode
    if mode != prev_mode:
        state['storage_mode_lock_until'] = tick + 2
    state['storage_mode'] = mode
    if mode == 'charge' and charge_room > 0.0 and charge_cap > 0.0 and current_surplus > 0.0:
        charge_total = min(total_charge_rate, charge_room, charge_cap, current_surplus)
    if mode == 'discharge' and discharge_cap > 0.0:
        desired = current_deficit + 0.25 * max(0.0, -next_balance)
        if chronic_deficit or current_profile.get('protect_bias', 0.0) > 0.5:
            desired += 0.10 * deficit_sum
        if severe_risk:
            desired *= 0.78
        if force_discharge:
            desired = max(desired, current_deficit + 0.50 * max(0.0, -next_balance))
        discharge_total = min(total_discharge_rate, discharge_cap, max(0.0, desired))
    allow_market_discharge = (
        tick >= game_length - ENDGAME_TICKS
        and total_soc > market_soc_floor
        and current_deficit < MIN_ORDER_VOLUME
        and signal >= -1.0
        and not severe_risk
        and loss_ratio < 0.30
        and current_profile.get('protect_bias', 0.0) <= 0.5
        and (next_risk_in is None or next_risk_in > 3)
        and market_sell_support
        and price_realism >= 0.72
        and not overpriced_market
        and anti_dump_headroom >= MIN_ORDER_VOLUME
    )
    if allow_market_discharge and mode != 'charge':
        extra = min(max(0.0, total_discharge_rate - discharge_total), max(0.0, discharge_cap - discharge_total))
        market_headroom = max(0.0, total_soc - market_soc_floor)
        if market_headroom > 0.0:
            discharge_for_market = min(extra, market_headroom)
            discharge_total += discharge_for_market
    charge_orders: List[Tuple[str, float]] = []
    discharge_orders: List[Tuple[str, float]] = []
    rem = charge_total
    for s in sorted(norm_storages, key=lambda x: x['soc']):
        if rem <= 1e-9:
            break
        room = max(0.0, cell_capacity - s['soc'])
        amt = min(rem, charge_rate, room)
        if amt >= 1e-9:
            charge_orders.append((s['id'], round_vol(amt)))
            rem -= amt
    rem = discharge_total
    floor_per_cell = discharge_floor_soc / max(1, len(norm_storages))
    for s in sorted(norm_storages, key=lambda x: -x['soc']):
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
        'soc_band': max(0.0, prep_soc - floor_soc),
        'emergency_floor_soc': emergency_floor_soc,
        'working_floor_soc': working_floor_soc,
        'high_risk_target_soc': high_risk_target_soc,
        'allow_market_discharge': allow_market_discharge,
        'mode': mode,
        'signal': signal,
        'protected_soc': protected_soc,
        'discharge_floor_soc': discharge_floor_soc,
    }


def analyze_topology(object_rows: List[Dict[str, Any]], network_rows: List[Dict[str, Any]], total_generated: float) -> Dict[str, Any]:
    def _decode(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value) if value else []
            except Exception:
                return None
        return value

    def _coerce_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            return None

    def _normalize_node_id(value: Any) -> Optional[str]:
        value = _decode(value)
        if isinstance(value, dict):
            load = value.get('load')
            idx = value.get('int')
        elif isinstance(value, (list, tuple)) and len(value) >= 2 and isinstance(value[0], str):
            load = value[0]
            idx = value[1]
        else:
            load = getattr(value, 'load', None)
            idx = getattr(value, 'int', None)
        idx = _coerce_int(idx)
        if load is None or idx is None:
            return None
        return f"{str(load).strip().lower()}:{idx}"

    def _looks_like_segment(value: Any) -> bool:
        value = _decode(value)
        if isinstance(value, dict):
            return 'line' in value and ('id' in value or ('load' in value and 'int' in value))
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            return _normalize_node_id(value[0]) is not None and _coerce_int(value[1]) is not None
        return getattr(value, 'id', None) is not None and _coerce_int(getattr(value, 'line', None)) is not None

    def _normalize_segment(value: Any) -> Optional[Tuple[str, int]]:
        value = _decode(value)
        if isinstance(value, dict):
            node_key = _normalize_node_id(value.get('id'))
            line = _coerce_int(value.get('line'))
        elif isinstance(value, (list, tuple)) and len(value) >= 2:
            node_key = _normalize_node_id(value[0])
            line = _coerce_int(value[1])
        else:
            node_key = _normalize_node_id(getattr(value, 'id', None))
            line = _coerce_int(getattr(value, 'line', None))
        if not node_key or line is None or line <= 0:
            return None
        return node_key, line

    def _normalize_route(value: Any) -> Optional[List[Tuple[str, int]]]:
        value = _decode(value)
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return None
        route: List[Tuple[str, int]] = []
        for segment in value:
            normalized = _normalize_segment(segment)
            if normalized is None:
                return None
            route.append(normalized)
        return route

    def _normalize_object_routes(value: Any) -> Tuple[List[List[Tuple[str, int]]], int, int]:
        value = _decode(value)
        if value is None:
            return [], 0, 1
        if not isinstance(value, (list, tuple)):
            return [], 0, 1
        if not value:
            return [], 1, 0
        if value and _looks_like_segment(value[0]):
            candidates = [value]
        else:
            candidates = list(value)
        routes: List[List[Tuple[str, int]]] = []
        empty_count = 0
        broken_count = 0
        for candidate in candidates:
            candidate = _decode(candidate)
            if candidate == [] or candidate == ():
                empty_count += 1
                continue
            route = _normalize_route(candidate)
            if route is None:
                broken_count += 1
                continue
            if not route:
                empty_count += 1
                continue
            routes.append(route)
        return routes, empty_count, broken_count

    def _route_key(route: List[Tuple[str, int]]) -> str:
        if not route:
            return 'root'
        return '>'.join(f'{node_key}:{line}' for node_key, line in route)

    def _prefix_keys(route: List[Tuple[str, int]]) -> List[str]:
        return [_route_key(route[:idx]) for idx in range(1, len(route))]

    def _route_is_rooted(route: List[Tuple[str, int]]) -> bool:
        return bool(route) and route[0][0].startswith('main:')

    def _route_has_cycle(route: List[Tuple[str, int]]) -> bool:
        nodes = [node_key for node_key, _ in route]
        return len(nodes) != len(set(nodes))

    def _first_route_depth(routes: List[List[Tuple[str, int]]]) -> int:
        return len(routes[0]) if routes else 0

    warnings: List[str] = []
    vulnerabilities: List[str] = []

    def _add_warning(warning: str, vulnerability: Optional[str] = None) -> None:
        if warning not in warnings:
            warnings.append(warning)
        target = vulnerability if vulnerability is not None else warning
        if target and target not in vulnerabilities:
            vulnerabilities.append(target)

    by_branch: Dict[str, Dict[str, float]] = {}
    branch_mix: Dict[str, Dict[str, int]] = {}
    object_path_depths: List[float] = []
    hospital_inputs = 0
    factory_inputs = 0
    rootless_network_routes: set = set()
    cyclic_network_routes: set = set()
    duplicate_network_routes: set = set()
    conflicting_network_routes: set = set()
    missing_prefix_routes: set = set()
    network_route_keys: set = set()
    broken_network_rows = 0
    segment_parent_map: Dict[str, set] = {}

    for row in network_rows:
        route = _normalize_route(row.get('location', []))
        if route is None:
            broken_network_rows += 1
            _add_warning('broken_network_routes', 'broken_network_routes')
            continue
        if not route:
            continue
        route_key = _route_key(route)
        if route_key in network_route_keys:
            duplicate_network_routes.add(route_key)
        network_route_keys.add(route_key)
        if not _route_is_rooted(route):
            rootless_network_routes.add(route_key)
        if _route_has_cycle(route):
            cyclic_network_routes.add(route_key)
        for idx, (node_key, line) in enumerate(route):
            segment_key = f'{node_key}:{line}'
            parent_key = _route_key(route[:idx]) if idx > 0 else '__root__'
            segment_parent_map.setdefault(segment_key, set()).add(parent_key)
        throughput = abs(safe_float(row.get('upflow', 0.0), 0.0)) + abs(safe_float(row.get('downflow', 0.0), 0.0))
        bucket = by_branch.setdefault(route_key, {'losses': 0.0, 'upflow': 0.0, 'downflow': 0.0, 'throughput': 0.0, 'count': 0})
        bucket['losses'] += safe_float(row.get('losses', 0.0), 0.0)
        bucket['upflow'] += safe_float(row.get('upflow', 0.0), 0.0)
        bucket['downflow'] += safe_float(row.get('downflow', 0.0), 0.0)
        bucket['throughput'] += throughput
        bucket['count'] += 1

    for route_key in list(network_route_keys):
        for idx in range(1, len(route_key.split('>'))):
            prefix_key = '>'.join(route_key.split('>')[:idx])
            if prefix_key not in network_route_keys:
                missing_prefix_routes.add(route_key)
                break

    for segment_key, parents in segment_parent_map.items():
        non_root_parents = {parent for parent in parents if parent != '__root__'}
        if len(non_root_parents) > 1:
            conflicting_network_routes.add(segment_key)

    if duplicate_network_routes:
        _add_warning('duplicate_network_routes', 'duplicate_network_routes')
    if conflicting_network_routes:
        _add_warning('conflicting_network_routes', 'conflicting_network_routes')
    if rootless_network_routes:
        _add_warning('routes_not_connected_to_main', 'routes_not_connected_to_main')
    if cyclic_network_routes:
        _add_warning('cyclic_routes', 'cyclic_routes')
    if missing_prefix_routes:
        _add_warning('disconnected_routes', 'disconnected_routes')

    def _is_valid_route(route: List[Tuple[str, int]]) -> bool:
        route_key = _route_key(route)
        if route_key not in network_route_keys:
            return False
        if not _route_is_rooted(route):
            return False
        if _route_has_cycle(route):
            return False
        return all(prefix_key in network_route_keys for prefix_key in _prefix_keys(route))

    islanded_objects: set = set()
    broken_objects: set = set()

    for row in object_rows:
        routes, empty_count, broken_count = _normalize_object_routes(row.get('path', []))
        object_path_depths.append(avg([len(route) for route in routes], default=0.0))
        address = row.get('address', 'unknown')
        typ = row.get('type')
        valid_route_keys = sorted({_route_key(route) for route in routes if _is_valid_route(route)})
        invalid_route_count = 0
        missing_network_route_count = 0
        for route in routes:
            route_key = _route_key(route)
            if route_key not in network_route_keys:
                missing_network_route_count += 1
                continue
            if not _is_valid_route(route):
                invalid_route_count += 1

        if typ != 'main' and (empty_count > 0 or broken_count > 0 or invalid_route_count > 0 or missing_network_route_count > 0):
            broken_objects.add(address)
            _add_warning('broken_object_paths', 'broken_object_paths')
            _add_warning(f'broken_path:{address}', f'broken_path:{address}')
        if typ != 'main' and empty_count > 0:
            _add_warning('empty_object_paths', 'empty_object_paths')
            _add_warning(f'empty_path:{address}', f'empty_path:{address}')
        if typ != 'main' and missing_network_route_count > 0:
            _add_warning('object_paths_not_in_network', 'object_paths_not_in_network')
            _add_warning(f'path_not_in_network:{address}', f'path_not_in_network:{address}')
        if typ != 'main' and not valid_route_keys:
            islanded_objects.add(address)
        if typ == 'hospital':
            hospital_inputs = max(hospital_inputs, len(valid_route_keys))
            if len(valid_route_keys) != 2:
                _add_warning('hospital_not_dual_fed', 'hospital_not_dual_fed')
        if typ == 'factory':
            factory_inputs = max(factory_inputs, len(valid_route_keys))
            if len(valid_route_keys) == 0:
                _add_warning('factory_missing_input', 'factory_missing_input')
            elif len(valid_route_keys) > 1:
                _add_warning('factory_overconnected', 'factory_overconnected')

        for route_key in valid_route_keys:
            mix = branch_mix.setdefault(route_key, {'gen': 0, 'load': 0, 'storage': 0, 'hospital': 0, 'factory': 0})
            if typ in ('solar', 'wind'):
                mix['gen'] += 1
            elif typ in ('houseA', 'houseB', 'office', 'factory', 'hospital'):
                mix['load'] += 1
            elif typ == 'storage':
                mix['storage'] += 1
            if typ == 'hospital':
                mix['hospital'] += 1
            if typ == 'factory':
                mix['factory'] += 1

    if islanded_objects:
        _add_warning('islanded_objects', 'islanded_objects')

    branch_losses_sorted = sorted(({'branch': key, **values} for key, values in by_branch.items()), key=lambda item: item['losses'], reverse=True)
    total_branch_losses = sum(item['losses'] for item in branch_losses_sorted)
    total_throughput = sum(item['throughput'] for item in branch_losses_sorted)
    branch_concentration = 0.0
    if total_throughput > 1e-9:
        branch_concentration = sum((item['throughput'] / total_throughput) ** 2 for item in branch_losses_sorted if item['throughput'] > 0.0)
    loss_share_est = total_branch_losses / max(total_generated, 1e-9) if total_generated > 1e-9 else 0.0
    expected_useful_energy = max(0.0, total_generated - total_branch_losses)
    if loss_share_est > 0.26:
        _add_warning('high_network_losses', 'loss_share_above_empirical_safe_zone')
    if branch_losses_sorted and total_branch_losses > 0.0 and branch_losses_sorted[0]['losses'] > 0.55 * total_branch_losses:
        _add_warning('losses_concentrated_in_one_branch', 'losses_concentrated_in_one_branch')
    if branch_concentration > 0.56:
        _add_warning('branch_flow_concentration', 'branch_flow_concentration')
    for branch, mix in branch_mix.items():
        if mix['gen'] > 0 and mix['load'] > 0:
            _add_warning(f'mixed_branch:{branch}', f'mixed_branch:{branch}')

    structural_fail = bool(
        broken_network_rows
        or rootless_network_routes
        or cyclic_network_routes
        or duplicate_network_routes
        or conflicting_network_routes
        or missing_prefix_routes
        or broken_objects
        or islanded_objects
    )
    return {
        'branch_losses': branch_losses_sorted[:10],
        'branch_mix': branch_mix,
        'warnings': warnings,
        'vulnerabilities': vulnerabilities,
        'branch_concentration_score': branch_concentration,
        'loss_share_est': loss_share_est,
        'expected_useful_energy': expected_useful_energy,
        'hospital_inputs': hospital_inputs,
        'factory_inputs': factory_inputs,
        'is_tree_like': not structural_fail,
        'avg_object_path_depth': avg(object_path_depths, default=0.0),
    }


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


def build_ladder(sell_volume: float, market_ref: float, fill_ratio: float, max_tickets: int, cfg: Dict[str, Any], buy_ref: Optional[float] = None, profile_ctx: Optional[Dict[str, Any]] = None, market_ctx: Optional[Dict[str, Any]] = None, startup_mode: bool = False, storage_excess: bool = False) -> List[Tuple[float, float]]:
    if sell_volume < MIN_ORDER_VOLUME:
        return []
    market_cap = clamp(safe_float(cfg.get('exchangeExternalBuy', MARKET_PRICE_MAX), MARKET_PRICE_MAX), MARKET_PRICE_MIN, MARKET_PRICE_MAX)
    gp_price = clamp(safe_float(cfg.get('exchangeExternalSell', MARKET_PRICE_MIN), MARKET_PRICE_MIN), MARKET_PRICE_MIN, market_cap)
    step = safe_float(cfg.get('exchangeConsumerPriceStep', 0.2), 0.2)
    max_tickets = max(0, safe_int(max_tickets, 0))
    buy_ref = safe_float(buy_ref, None)
    current = profile_ctx.get('current', {}) if profile_ctx else {}
    avg_combo = safe_float(profile_ctx.get('avg_combo_12', 0.0), 0.0) if profile_ctx else 0.0
    avg_risk = safe_float(profile_ctx.get('avg_risk_12', 0.0), 0.0) if profile_ctx else 0.0
    price_realism = safe_float(market_ctx.get('price_realism', 1.0), 1.0) if market_ctx else 1.0
    overpriced = bool(market_ctx.get('overpriced', False)) if market_ctx else False
    has_fill_history = bool(market_ctx.get('has_fill_history', False)) if market_ctx else False
    weak_fill = bool(market_ctx.get('weak_fill', False)) if market_ctx else fill_ratio < safe_float(MARKET_INSIGHTS.get('weak_fill_ratio', 0.78), 0.78)
    base_ref = market_ref
    if buy_ref is not None and has_fill_history and not weak_fill and not overpriced and price_realism >= 0.82:
        buy_anchor = clamp(buy_ref, market_ref - 2.0 * step, market_ref + 1.5 * step)
        base_ref = 0.85 * base_ref + 0.15 * buy_anchor
    if fill_ratio < 0.55:
        base_ref -= 1.2 * step
    elif fill_ratio > 0.90:
        base_ref += 1.0 * step
    if current.get('mixed_peak') or avg_combo > avg_risk + 0.10:
        base_ref += 0.8 * step
    if current.get('protect_bias', 0.0) > 0.5 or avg_risk > avg_combo + 0.15:
        base_ref -= 0.6 * step
    if weak_fill or overpriced:
        base_ref = min(base_ref, safe_float(MARKET_INSIGHTS.get('preferred_ask_high', 4.6), 4.6))
    if price_realism < 0.75:
        base_ref = min(base_ref, safe_float(MARKET_INSIGHTS.get('preferred_ask_high', 4.6), 4.6))
    base_ref = clamp(base_ref, MARKET_PRICE_MIN, market_cap)

    if startup_mode or weak_fill:
        prices = [max(MARKET_PRICE_MIN, gp_price + step, base_ref - step), min(base_ref + 2 * step, market_cap)]
        shares = [0.85, 0.15]
    elif storage_excess:
        prices = [max(MARKET_PRICE_MIN, gp_price + step, base_ref - step), min(base_ref + 2 * step, market_cap), min(base_ref + 4 * step, market_cap)]
        shares = [0.60, 0.25, 0.15]
    else:
        prices = [max(MARKET_PRICE_MIN, gp_price + step, base_ref - step), min(base_ref + 2 * step, market_cap), min(base_ref + 4 * step, market_cap)]
        shares = [0.70, 0.20, 0.10]
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


def compute_safe_sell_volume(state: Dict[str, Any], object_rows: List[Dict[str, Any]], marketable_useful_now: float, offer_cap: float, reserve: float, balance_now: float, topology: Dict[str, Any], market_ctx: Dict[str, Any]) -> float:
    gen_total = sum(r['generated'] for r in object_rows)
    wind_total = sum(r['generated'] for r in object_rows if r.get('type') == 'wind')
    wind_share = wind_total / max(gen_total, 1e-9) if gen_total > 0.0 else 0.0
    loss_ratio = safe_float(state.get('loss_ratio_ewma', 0.18), 0.18)
    abs_err = safe_float(state.get('abs_err_ewma', 1.2), 1.2)
    recent_fill = safe_float(market_ctx.get('recent_fill_ratio', safe_float(state.get('fill_ratio_ewma', 0.84), 0.84)), 0.84)
    price_realism = safe_float(market_ctx.get('price_realism', 1.0), 1.0)
    has_fill_history = bool(market_ctx.get('has_fill_history', False))
    uncertainty = 0.22 * abs_err + 0.12 * wind_share * marketable_useful_now
    uncertainty += 0.10 * max(0.0, loss_ratio - 0.12) * marketable_useful_now
    if 'high_network_losses' in topology.get('warnings', []):
        uncertainty += 0.08 * marketable_useful_now
    safe_target = max(0.0, min(offer_cap, marketable_useful_now) - max(reserve, uncertainty))
    if has_fill_history and recent_fill < safe_float(MARKET_INSIGHTS.get('weak_fill_ratio', 0.78), 0.78):
        safe_target *= clamp(0.45 + 0.70 * recent_fill, 0.30, 0.95)
    if price_realism < 0.70:
        safe_target *= clamp(price_realism, 0.25, 1.0)
    if balance_now < 0.0:
        safe_target = min(safe_target, max(0.0, balance_now + marketable_useful_now))
    safe_target = round_vol(max(0.0, min(offer_cap, marketable_useful_now, safe_target)))
    return safe_target if safe_target >= MIN_ORDER_VOLUME else 0.0


def current_theoretical_metrics(state: Dict[str, Any], object_rows: List[Dict[str, Any]], weather: Dict[str, float], bundle: Dict[str, Dict[str, Any]], tick: int, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    sun_now = weather['sun']
    wind_now = weather['wind']
    cfg = cfg or state.get('cfg_runtime', {})
    sun_spread = max(get_forecast_spread(bundle, 'sun', 0.0), safe_float(cfg.get('corridorSun', 0.5), 0.5))
    wind_spread = max(get_forecast_spread(bundle, 'wind', 0.0), safe_float(cfg.get('corridorWind', 0.5), 0.5))
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
        load_model = predict_object_load(state, row, load_forecast, tick) if fc_name else 0.0
        totals['load_model_now'] += load_model
    totals['loss_theoretical_now'] = predict_total_losses(state, totals['gross_theoretical_now'], totals['load_model_now'])
    return totals


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


def log_tick_data(summary_row: Dict[str, Any], strategy_row: Dict[str, Any]) -> None:
    ensure_dir(LOG_DIR)
    write_jsonl(TICK_SUMMARY_FILE, summary_row)
    write_jsonl(STRATEGY_DEBUG_FILE, strategy_row)


def profile_log_fields(profile_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'profile_current': profile_ctx.get('current', {}),
        'profile_next_mixed_in': profile_ctx.get('next_mixed_in'),
        'profile_next_risk_in': profile_ctx.get('next_risk_in'),
    }


def market_log_fields(market_stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
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
    }


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
    forecast_bundle = get_forecast_bundle(psm, game_length=game_length, cfg=cfg, object_rows=object_rows)
    state['cfg_runtime'] = cfg
    state['weather_runtime'] = weather
    refresh_static_runtime_context(state, object_rows)
    apply_startup_observation(
        state,
        object_rows,
        forecast_bundle,
        tick,
        total_consumed=total_consumed,
    )

    forecast_profile = build_forecast_profile(state, forecast_bundle, object_rows, game_length)
    profile_ctx = forecast_profile_context(forecast_profile, tick, horizon=max(12, LOOKAHEAD * 2))
    future = forecast_window(state, object_rows, forecast_bundle, tick, game_length, LOOKAHEAD)
    current_theoretical = current_theoretical_metrics(state, object_rows, weather, forecast_bundle, tick, cfg)

    useful_raw = compute_useful_energy(total_generated, total_consumed, total_losses)
    useful_now = max(0.0, useful_raw)
    balance_now = compute_balance_energy(total_generated, total_consumed, total_losses)

    market_stats = analyze_exchange(exchange_rows)
    sell_avg_price = market_stats.get('sell_avg_contracted_price')
    buy_ref = market_stats.get('buy_avg_contracted_price') or market_stats.get('buy_avg_asked_price')
    fill_ratio_now = market_stats.get('sell_fill_ratio')
    exch_log = get_exchange_log(psm)
    if sell_avg_price is not None:
        state['market_ref'] = 0.76 * safe_float(state.get('market_ref', 4.8), 4.8) + 0.24 * sell_avg_price
    elif buy_ref is not None:
        prev_ref = safe_float(state.get('market_ref', 4.8), 4.8)
        capped_buy_ref = min(safe_float(buy_ref, prev_ref), prev_ref + 1.2)
        state['market_ref'] = 0.88 * prev_ref + 0.12 * capped_buy_ref
    elif tick > 0 and tick - 1 < len(exch_log):
        last_log_price = safe_float(exch_log[tick - 1], state.get('market_ref', 4.8))
        state['market_ref'] = 0.88 * safe_float(state.get('market_ref', 4.8), 4.8) + 0.12 * last_log_price
    if fill_ratio_now is not None:
        state['fill_ratio_ewma'] = 0.76 * safe_float(state.get('fill_ratio_ewma', 0.84), 0.84) + 0.24 * fill_ratio_now
    update_market_history(state, tick, market_stats)
    market_ctx = build_market_context(state, market_stats)
    anti_dump_cap_preview = compute_offer_cap(state, cfg, tick, useful_now)
    market_ctx['anti_dump_cap'] = anti_dump_cap_preview
    market_ctx['anti_dump_headroom'] = max(0.0, anti_dump_cap_preview - safe_float(state.get('last_sell_volume', 0.0), 0.0))
    decision_loss_ratio = safe_float(state.get('loss_ratio_ewma', 0.18), 0.18)
    decision_abs_err = safe_float(state.get('abs_err_ewma', 1.2), 1.2)

    charge_orders, discharge_orders, battery_dbg = storage_policy(
        state, cfg, obj_agg['storages'], balance_now, useful_now, future,
        safe_float(market_ctx.get('recent_fill_ratio', state.get('fill_ratio_ewma', 0.84)), 0.84), tick, game_length,
        loss_ratio=decision_loss_ratio,
        profile_ctx=profile_ctx,
        market_ctx=market_ctx,
    )
    startup_mode = startup_active(state, tick)
    stable_surplus_now = max(0.0, balance_now)
    gross_marketable_useful_now = max(0.0, balance_now + battery_dbg['discharge_for_market'] - battery_dbg['charge_total'])
    stress_sell_mode = bool(
        balance_now < 0.0
        or decision_loss_ratio > 0.30
        or profile_ctx.get('current', {}).get('protect_bias', 0.0) > 0.5
        or 'high_network_losses' in topology.get('warnings', [])
    )
    marketable_useful_now = gross_marketable_useful_now
    if battery_dbg.get('mode') == 'charge':
        marketable_useful_now = min(marketable_useful_now, max(0.0, stable_surplus_now - battery_dbg['charge_total']))
    else:
        marketable_useful_now = min(marketable_useful_now, stable_surplus_now + battery_dbg['discharge_for_market'])
    if stress_sell_mode:
        marketable_useful_now = min(
            marketable_useful_now,
            max(0.0, balance_now + battery_dbg['discharge_for_market'] - battery_dbg['charge_total']),
        )
    offer_cap = compute_offer_cap(state, cfg, tick, marketable_useful_now)
    reserve = compute_reserve(state, future, object_rows, total_losses, marketable_useful_now, profile_ctx=profile_ctx)
    market_ctx['anti_dump_cap'] = offer_cap
    market_ctx['anti_dump_headroom'] = max(0.0, offer_cap - safe_float(state.get('last_sell_volume', 0.0), 0.0))
    sell_volume = compute_safe_sell_volume(state, object_rows, marketable_useful_now, offer_cap, reserve, balance_now, topology, market_ctx)
    if startup_mode and battery_dbg.get('signal', 0.0) <= reserve + 0.5:
        sell_volume = 0.0
    elif marketable_useful_now >= MIN_ORDER_VOLUME and sell_volume <= 0.0 and market_ctx.get('good_fill') and safe_float(market_ctx.get('price_realism', 1.0), 1.0) >= 0.85:
        sell_volume = round_vol(min(offer_cap, marketable_useful_now, max(MIN_ORDER_VOLUME, 0.48 * marketable_useful_now)))

    storage_excess = battery_dbg['total_soc'] > battery_dbg.get('prep_soc', battery_dbg['target_soc']) + 0.08 * len(obj_agg['storages']) * safe_float(cfg['cellCapacity'], 120.0)
    ladder = build_ladder(
        sell_volume,
        safe_float(state.get('market_ref', 4.8), 4.8),
        safe_float(market_ctx.get('recent_fill_ratio', state.get('fill_ratio_ewma', 0.84)), 0.84),
        safe_int(cfg['exchangeMaxTickets'], 100),
        cfg,
        buy_ref=buy_ref,
        profile_ctx=profile_ctx,
        market_ctx=market_ctx,
        startup_mode=startup_mode,
        storage_excess=storage_excess,
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

    forecast_meta = forecast_bundle.get('_meta', {})
    score_breakdown = parse_score_breakdown(get_raw_score_delta(psm))
    score_income = safe_float(score_breakdown.get('income', 0.0), 0.0)
    score_loss_only = safe_float(score_breakdown.get('loss', 0.0), 0.0)
    total_score = safe_float(score_breakdown.get('total_score', None), None)
    if total_score is None:
        total_score = get_total_score(psm)
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
        'discharge_for_market': round(safe_float(battery_dbg.get('discharge_for_market', 0.0), 0.0), 6),
        'target_soc': round(battery_dbg['target_soc'], 6),
        'prep_soc': round(safe_float(battery_dbg.get('prep_soc', battery_dbg['target_soc']), battery_dbg['target_soc']), 6),
        'total_soc': round(battery_dbg['total_soc'], 6),
        'emergency_floor_soc': round(safe_float(battery_dbg.get('emergency_floor_soc', 0.0), 0.0), 6),
        'working_floor_soc': round(safe_float(battery_dbg.get('working_floor_soc', 0.0), 0.0), 6),
        'high_risk_target_soc': round(safe_float(battery_dbg.get('high_risk_target_soc', battery_dbg['target_soc']), battery_dbg['target_soc']), 6),
        'market_ref': round(safe_float(state.get('market_ref', 4.8), 4.8), 6),
        'fill_ratio_ewma': round(safe_float(state.get('fill_ratio_ewma', 0.84), 0.84), 6),
        'market_price_realism': round(safe_float(market_ctx.get('price_realism', 1.0), 1.0), 6),
        'market_overpriced': bool(market_ctx.get('overpriced', False)),
        'storage_mode': battery_dbg.get('mode'),
        'storage_signal': round(safe_float(battery_dbg.get('signal', 0.0), 0.0), 6),
        'startup_load_scale': round(startup_scale(state, tick), 6),
        'ladder': ladder,
        'topology_warnings': topology.get('warnings', []),
        'topology_branch_concentration': round(safe_float(topology.get('branch_concentration_score', 0.0), 0.0), 6),
        'topology_loss_share_est': round(safe_float(topology.get('loss_share_est', 0.0), 0.0), 6),
        'topology_vulnerabilities': topology.get('vulnerabilities', []),
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
    summary_row.update(profile_log_fields(profile_ctx))
    summary_row.update(market_log_fields(market_stats))
    strategy_row = {
        'tick': tick,
        'weather': weather,
        'forecast_meta': forecast_meta,
        'score_breakdown_debug': {
            'format': score_breakdown.get('format'),
            'ambiguous': bool(score_breakdown.get('ambiguous', False)),
        },
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
        'forecast_profile_context': profile_ctx,
        'future_window': future,
        'topology': topology,
        'market_context': market_ctx,
        'physical_balance_now': balance_now,
        'useful_supply_now': useful_now,
        'gross_marketable_useful_now': gross_marketable_useful_now,
        'marketable_useful_now': marketable_useful_now,
        'stress_sell_mode': stress_sell_mode,
        'startup_mode': startup_mode,
        'startup_load_scale': startup_scale(state, tick),
        'reserve_formula': {
            'abs_err_ewma': decision_abs_err,
            'loss_ratio_ewma': decision_loss_ratio,
            'reserve': reserve,
        },
        'offer_cap': offer_cap,
        'battery': battery_dbg,
        'charge_orders': charge_orders,
        'discharge_orders': discharge_orders,
        'sell_volume': sell_volume,
        'ladder': ladder,
    }
    log_tick_data(summary_row, strategy_row)
    apply_post_tick_learning(
        state,
        object_rows,
        weather,
        forecast_bundle,
        tick,
        cfg=cfg,
        total_consumed=total_consumed,
        total_losses=total_losses,
        marketable_useful_now=marketable_useful_now,
        total_generated=total_generated,
    )
    state['prev_useful_supply_est'] = marketable_useful_now
    state['prev_useful_energy_actual'] = useful_now
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
