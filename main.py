import json
import os
import traceback
from typing import Any, Dict, List, Tuple

import ips


STATE_FILE = "ies_state.json"
REPORT_FILE = "controller_report.json"
MIN_ORDER_VOLUME = 0.25
MIN_PRICE = 4.0
MAX_PRICE = 20.0
MODEL_EWMA_ALPHA = 0.18
ERR_EWMA_ALPHA = 0.25
MIN_RESERVE_SHARE = 0.10
MAX_RESERVE_SHARE = 0.72

CONSUMER_FORECAST_MAP = {
    "hospital": "hospital",
    "factory": "factory",
    "office": "office",
    "housea": "houseA",
    "houseb": "houseB",
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def round_vol(value: float) -> float:
    return round(max(0.0, safe_float(value, 0.0)), 3)


def round_price(value: float, step: float, low: float, high: float) -> float:
    step = max(0.01, safe_float(step, 0.2))
    value = clamp(safe_float(value, low), low, high)
    return round(round(value / step) * step, 2)


def mean(values: List[float], default: float = 0.0) -> float:
    if not values:
        return float(default)
    return sum(values) / len(values)


def load_state() -> Dict[str, Any]:
    path = os.path.join(os.getcwd(), STATE_FILE)
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state: Dict[str, Any], tick: int, useful_energy: float) -> None:
    path = os.path.join(os.getcwd(), STATE_FILE)
    payload: Dict[str, Any] = state if isinstance(state, dict) else {}
    payload["tick"] = int(tick)
    payload["useful"] = round(max(0.0, safe_float(useful_energy, 0.0)), 6)
    try:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False)
    except Exception:
        pass


def save_report(report: Dict[str, Any]) -> None:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), REPORT_FILE)
    try:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2)
    except Exception:
        pass


def anti_dump_limit(state: Dict[str, Any], tick: int) -> float:
    prev_tick = int(safe_float(state.get("tick"), -1))
    prev_useful = 0.0
    if prev_tick == tick - 1:
        prev_useful = max(0.0, safe_float(state.get("useful"), 0.0))
    return 1.2 * prev_useful + 10.0


def storage_rows(psm: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for obj in getattr(psm, "objects", []):
        if str(getattr(obj, "type", "")).lower() != "storage":
            continue
        if safe_float(getattr(obj, "failed", 0), 0.0) > 0:
            continue
        address = getattr(obj, "address", None)
        if isinstance(address, (list, tuple)) and address:
            storage_id = address[0]
        else:
            storage_id = str(getattr(obj, "id", "storage"))
        soc = safe_float(getattr(getattr(obj, "charge", None), "now", 0.0), 0.0)
        rows.append({"id": storage_id, "soc": max(0.0, soc)})
    return rows


def current_balance(psm: Any) -> Dict[str, float]:
    total_power = getattr(psm, "total_power", None)
    generated = safe_float(getattr(total_power, "generated", 0.0), 0.0)
    consumed = safe_float(getattr(total_power, "consumed", 0.0), 0.0)
    losses = safe_float(getattr(total_power, "losses", 0.0), 0.0)
    # Доступный остаток энергии на текущем тике: генерация минус потребление и потери.
    balance_after_consumption = generated - consumed - losses
    useful = max(0.0, balance_after_consumption)
    return {
        "generated": generated,
        "consumed": consumed,
        "losses": losses,
        "useful": useful,
        "balance": balance_after_consumption,
    }


def next_tick_consumption(psm: Any) -> float:
    tick = int(getattr(psm, "tick", 0)) + 1
    forecasts = getattr(psm, "forecasts", None)
    total = 0.0
    for obj in getattr(psm, "objects", []):
        kind = str(getattr(obj, "type", "")).lower()
        forecast_name = CONSUMER_FORECAST_MAP.get(kind)
        if not forecast_name:
            continue
        series = getattr(forecasts, forecast_name, [])
        if not series:
            continue
        idx = max(0, min(tick, len(series) - 1))
        total += safe_float(series[idx], 0.0)
    return total


def state_section(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    section = state.get(key)
    if not isinstance(section, dict):
        section = {}
        state[key] = section
    return section


def object_key(obj: Any, fallback: str) -> str:
    address = getattr(obj, "address", None)
    if isinstance(address, (list, tuple)) and address:
        return str(address[0])
    return str(getattr(obj, "id", fallback))


def generation_rows(psm: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for obj in getattr(psm, "objects", []):
        kind = str(getattr(obj, "type", "")).lower()
        if kind not in {"solar", "wind"}:
            continue
        power_now = getattr(getattr(obj, "power", None), "now", None)
        rotation_now = safe_float(getattr(getattr(obj, "windRotation", None), "now", 0.0), 0.0)
        rows.append(
            {
                "key": object_key(obj, kind),
                "type": kind,
                "generated": max(0.0, safe_float(getattr(power_now, "generated", 0.0), 0.0)),
                "failed": int(safe_float(getattr(obj, "failed", 0), 0.0)),
                "wind_rotation": max(0.0, rotation_now),
            }
        )
    return rows


def get_generation_model(state: Dict[str, Any], key: str, kind: str) -> Dict[str, Any]:
    models = state_section(state, "generation_models")
    model = models.get(key)
    if isinstance(model, dict) and str(model.get("kind", "")) == kind:
        return model
    if kind == "solar":
        model = {"kind": "solar", "factor": 0.65, "strength": 1.0, "err": 0.8, "samples": 0}
    else:
        model = {
            "kind": "wind",
            "factor": 0.0048,
            "rot_factor": 80.0,
            "wind_to_rot": 0.04,
            "strength": 1.0,
            "err": 2.5,
            "last_failed": 0,
            "samples": 0,
        }
    models[key] = model
    return model


def update_generation_models(state: Dict[str, Any], rows: List[Dict[str, Any]], psm: Any) -> None:
    cfg = getattr(psm, "config", {})
    sun_now = max(0.0, safe_float(getattr(getattr(psm, "sun", None), "now", 0.0), 0.0))
    wind_now = max(0.0, safe_float(getattr(getattr(psm, "wind", None), "now", 0.0), 0.0))
    max_solar = max(1.0, safe_float(cfg.get("maxSolarPower", 20.0), 20.0))
    max_wind = max(1.0, safe_float(cfg.get("maxWindPower", 20.0), 20.0))

    for row in rows:
        kind = row["type"]
        model = get_generation_model(state, row["key"], kind)
        generated = max(0.0, safe_float(row.get("generated", 0.0), 0.0))

        if kind == "solar":
            factor = safe_float(model.get("factor", 0.65), 0.65)
            strength = safe_float(model.get("strength", 1.0), 1.0)
            if sun_now > 0.15:
                measured_factor = generated / max(sun_now, 1e-6)
                factor = clamp((1.0 - MODEL_EWMA_ALPHA) * factor + MODEL_EWMA_ALPHA * measured_factor, 0.12, 3.0)

            baseline = max(0.0, factor * sun_now)
            if baseline > 0.2:
                ratio = generated / max(baseline, 1e-6)
                strength = clamp((1.0 - MODEL_EWMA_ALPHA) * strength + MODEL_EWMA_ALPHA * ratio, 0.55, 1.55)

            expected = baseline * strength
            err = abs(generated - expected)
            prev_err = safe_float(model.get("err", 0.8), 0.8)
            model["factor"] = factor
            model["strength"] = strength
            model["err"] = (1.0 - ERR_EWMA_ALPHA) * prev_err + ERR_EWMA_ALPHA * err
            model["samples"] = int(safe_float(model.get("samples", 0), 0.0)) + 1
            continue

        factor = safe_float(model.get("factor", 0.0048), 0.0048)
        rot_factor = safe_float(model.get("rot_factor", 80.0), 80.0)
        wind_to_rot = safe_float(model.get("wind_to_rot", 0.04), 0.04)
        strength = safe_float(model.get("strength", 1.0), 1.0)
        rotation_now = max(0.0, safe_float(row.get("wind_rotation", 0.0), 0.0))

        if wind_now > 0.8:
            measured_factor = generated / max(wind_now**3, 1e-6)
            factor = clamp((1.0 - MODEL_EWMA_ALPHA) * factor + MODEL_EWMA_ALPHA * measured_factor, 0.0008, 0.03)

        if rotation_now > 0.03:
            measured_rot_factor = generated / max(rotation_now**3, 1e-6)
            rot_factor = clamp((1.0 - MODEL_EWMA_ALPHA) * rot_factor + MODEL_EWMA_ALPHA * measured_rot_factor, 1.0, 700.0)
            if wind_now > 0.8:
                measured_ratio = rotation_now / max(wind_now, 1e-6)
                wind_to_rot = clamp((1.0 - MODEL_EWMA_ALPHA) * wind_to_rot + MODEL_EWMA_ALPHA * measured_ratio, 0.005, 0.22)

        direct = factor * wind_now**3
        rot_component = rot_factor * rotation_now**3
        expected = (0.6 * direct + 0.4 * rot_component) * strength
        if expected > 0.2:
            ratio = generated / max(expected, 1e-6)
            strength = clamp((1.0 - MODEL_EWMA_ALPHA) * strength + MODEL_EWMA_ALPHA * ratio, 0.50, 1.70)

        err = abs(generated - expected)
        prev_err = safe_float(model.get("err", 2.5), 2.5)
        model["factor"] = factor
        model["rot_factor"] = rot_factor
        model["wind_to_rot"] = wind_to_rot
        model["strength"] = strength
        model["err"] = (1.0 - ERR_EWMA_ALPHA) * prev_err + ERR_EWMA_ALPHA * err
        model["last_failed"] = 1 if int(row.get("failed", 0)) > 0 else 0
        model["samples"] = int(safe_float(model.get("samples", 0), 0.0)) + 1
        if generated > 0.0:
            model["max_power_seen"] = max(safe_float(model.get("max_power_seen", 0.0), 0.0), min(generated, max_wind))

        if int(row.get("failed", 0)) > 0:
            model["strength"] = clamp(0.90 * safe_float(model.get("strength", 1.0), 1.0), 0.40, 1.60)
        else:
            model["strength"] = clamp(safe_float(model.get("strength", 1.0), 1.0), 0.50, 1.70)

        model["factor"] = clamp(safe_float(model.get("factor", factor), factor), 0.0008, 0.03)
        model["rot_factor"] = clamp(safe_float(model.get("rot_factor", rot_factor), rot_factor), 1.0, 700.0)
        model["wind_to_rot"] = clamp(safe_float(model.get("wind_to_rot", wind_to_rot), wind_to_rot), 0.005, 0.22)
        model["err"] = clamp(safe_float(model.get("err", prev_err), prev_err), 0.0, max_wind)

    state_section(state, "generation_models")


def forecast_value(series: Any, tick: int, fallback: float = 0.0) -> float:
    if not series:
        return safe_float(fallback, 0.0)
    idx = max(0, min(int(tick), len(series) - 1))
    return safe_float(series[idx], fallback)


def predict_next_generation(psm: Any, state: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, float]:
    cfg = getattr(psm, "config", {})
    forecasts = getattr(psm, "forecasts", None)
    tick = int(getattr(psm, "tick", 0)) + 1

    sun_now = max(0.0, safe_float(getattr(getattr(psm, "sun", None), "now", 0.0), 0.0))
    wind_now = max(0.0, safe_float(getattr(getattr(psm, "wind", None), "now", 0.0), 0.0))
    fc_sun = forecast_value(getattr(forecasts, "sun", []), tick, sun_now)
    fc_wind = forecast_value(getattr(forecasts, "wind", []), tick, wind_now)
    sun_spread = max(0.0, safe_float(getattr(getattr(forecasts, "sun", None), "spread", 0.0), 0.0))
    wind_spread = max(0.0, safe_float(getattr(getattr(forecasts, "wind", None), "spread", 0.0), 0.0))
    max_solar = max(1.0, safe_float(cfg.get("maxSolarPower", 20.0), 20.0))
    max_wind = max(1.0, safe_float(cfg.get("maxWindPower", 20.0), 20.0))
    wind_limit = max(1.0, safe_float(cfg.get("weatherMaxWind", 15.0), 15.0))

    delay = max(0, int(safe_float(cfg.get("weatherEffectsDelay", 0), 0.0)))
    blend = 1.0 / max(1.0, float(delay + 1))

    total_gen = 0.0
    wind_gen = 0.0
    wind_rot_gap_sum = 0.0
    wind_err_sum = 0.0
    wind_failed = 0
    wind_count = 0

    for row in rows:
        kind = row["type"]
        model = get_generation_model(state, row["key"], kind)
        failed_now = int(safe_float(row.get("failed", 0), 0.0))

        if kind == "solar":
            eff_sun = max(0.0, (1.0 - blend) * sun_now + blend * max(0.0, fc_sun - 0.20 * sun_spread))
            factor = safe_float(model.get("factor", 0.65), 0.65)
            strength = clamp(safe_float(model.get("strength", 1.0), 1.0), 0.55, 1.55)
            err = safe_float(model.get("err", 0.8), 0.8)
            pred = factor * eff_sun * strength
            pred *= clamp(1.0 - 0.03 * err, 0.68, 1.0)
            if failed_now > 0:
                pred *= 0.55
            pred = clamp(pred, 0.0, max_solar)
            total_gen += pred
            continue

        wind_count += 1
        rotation_now = max(0.0, safe_float(row.get("wind_rotation", 0.0), 0.0))
        eff_wind = max(0.0, (1.0 - blend) * wind_now + blend * max(0.0, fc_wind - 0.35 * wind_spread))
        inertia = clamp(1.0 - 1.0 / max(2.0, float(delay + 2)), 0.45, 0.88)
        wind_to_rot = clamp(safe_float(model.get("wind_to_rot", 0.04), 0.04), 0.005, 0.22)
        projected_rot = max(0.0, inertia * rotation_now + (1.0 - inertia) * wind_to_rot * eff_wind)

        direct = clamp(safe_float(model.get("factor", 0.0048), 0.0048), 0.0008, 0.03) * eff_wind**3
        rot_based = clamp(safe_float(model.get("rot_factor", 80.0), 80.0), 1.0, 700.0) * projected_rot**3
        err = safe_float(model.get("err", 2.5), 2.5)
        strength = clamp(safe_float(model.get("strength", 1.0), 1.0), 0.50, 1.70)

        pred = (0.62 * direct + 0.38 * rot_based) * strength
        pred *= clamp(1.0 - 0.05 * err, 0.55, 1.0)
        if failed_now > 0:
            pred *= 0.55
            wind_failed += 1
        elif int(safe_float(model.get("last_failed", 0), 0.0)) > 0:
            pred *= 0.82
        if eff_wind > 0.85 * wind_limit:
            pred *= 0.9
        pred = clamp(pred, 0.0, max_wind)
        max_seen = safe_float(model.get("max_power_seen", 0.0), 0.0)
        if max_seen > 0.0:
            pred = min(pred, min(max_wind, 1.1 * max_seen + 0.4))

        wind_rot_gap_sum += abs(projected_rot - rotation_now)
        wind_err_sum += err
        wind_gen += pred
        total_gen += pred

    spread_norm = clamp(wind_spread / max(1.0, wind_limit), 0.0, 1.5)
    err_norm = clamp((wind_err_sum / max(1, wind_count)) / max(1.0, max_wind), 0.0, 1.5)
    fail_norm = (wind_failed / max(1, wind_count)) if wind_count > 0 else 0.0
    rot_norm = clamp((wind_rot_gap_sum / max(1, wind_count)) / 0.35, 0.0, 1.5)
    wind_risk = clamp(0.35 * spread_norm + 0.25 * err_norm + 0.25 * fail_norm + 0.15 * rot_norm, 0.0, 1.5)

    return {
        "total_gen_pred": total_gen,
        "wind_gen_pred": wind_gen,
        "sun_forecast": fc_sun,
        "wind_forecast": fc_wind,
        "wind_risk": wind_risk,
    }


def network_pressure_risk(psm: Any) -> Dict[str, float]:
    networks = getattr(psm, "networks", {})
    throughputs: List[float] = []
    losses: List[float] = []
    for line in getattr(networks, "values", lambda: [])():
        upflow = abs(safe_float(getattr(line, "upflow", 0.0), 0.0))
        downflow = abs(safe_float(getattr(line, "downflow", 0.0), 0.0))
        throughputs.append(upflow + downflow)
        losses.append(max(0.0, safe_float(getattr(line, "losses", 0.0), 0.0)))

    if not throughputs:
        return {"risk": 0.0, "pressure_ratio": 1.0, "loss_concentration": 0.0}

    avg_tp = mean(throughputs, 0.0)
    max_tp = max(throughputs)
    pressure_ratio = max_tp / max(avg_tp, 1e-6) if avg_tp > 0.0 else 1.0
    total_losses = sum(losses)
    loss_concentration = max(losses) / total_losses if total_losses > 1e-9 else 0.0

    pressure_risk = clamp((pressure_ratio - 1.5) / 1.1, 0.0, 1.5)
    concentration_risk = clamp((loss_concentration - 0.45) / 0.45, 0.0, 1.5)
    risk = clamp(0.65 * pressure_risk + 0.35 * concentration_risk, 0.0, 1.5)
    return {"risk": risk, "pressure_ratio": pressure_ratio, "loss_concentration": loss_concentration}


def update_loss_model(state: Dict[str, Any], balance_now: Dict[str, float]) -> None:
    loss_model = state_section(state, "loss_model")
    generated = max(0.0, safe_float(balance_now.get("generated", 0.0), 0.0))
    losses = max(0.0, safe_float(balance_now.get("losses", 0.0), 0.0))
    ratio_now = losses / generated if generated > 1e-6 else safe_float(loss_model.get("ratio_ewma", 0.12), 0.12)
    ratio_prev = safe_float(loss_model.get("ratio_ewma", ratio_now), ratio_now)
    ratio_ewma = (1.0 - ERR_EWMA_ALPHA) * ratio_prev + ERR_EWMA_ALPHA * ratio_now
    loss_model["ratio_ewma"] = clamp(ratio_ewma, 0.0, 0.65)


def predict_next_losses(
    state: Dict[str, Any],
    balance_now: Dict[str, float],
    generation_next: float,
    consumption_next: float,
    network_risk: float,
) -> float:
    loss_model = state_section(state, "loss_model")
    ratio_base = clamp(safe_float(loss_model.get("ratio_ewma", 0.12), 0.12), 0.0, 0.60)
    if generation_next <= 1e-6:
        return 0.0

    load_ratio = consumption_next / max(generation_next, 1e-6)
    stress_multiplier = 1.0 + 0.22 * max(0.0, load_ratio - 0.95)
    network_multiplier = 1.0 + 0.35 * clamp(network_risk, 0.0, 1.5)
    forecast_ratio = clamp(ratio_base * stress_multiplier * network_multiplier, 0.0, 0.65)
    predicted = generation_next * forecast_ratio

    current_generated = max(0.0, safe_float(balance_now.get("generated", 0.0), 0.0))
    if current_generated > 1e-6:
        scaled_current = max(0.0, safe_float(balance_now.get("losses", 0.0), 0.0)) * (generation_next / current_generated)
        predicted = 0.55 * predicted + 0.45 * scaled_current
    return max(0.0, predicted)


def compute_dynamic_reserve(
    total_capacity: float,
    balance_now: Dict[str, float],
    next_consumption: float,
    generation_next: float,
    losses_next: float,
    wind_risk: float,
    network_risk: float,
    market_fill: float,
    anti_dump_cap: float,
) -> float:
    next_balance = generation_next - next_consumption - losses_next
    expected_surplus = max(0.0, next_balance)
    anti_dump_pressure = 0.0
    if expected_surplus >= MIN_ORDER_VOLUME:
        anti_dump_pressure = clamp((expected_surplus - anti_dump_cap) / max(expected_surplus, 1.0), 0.0, 1.0)

    reserve = MIN_RESERVE_SHARE * total_capacity
    reserve += 0.55 * max(0.0, -next_balance)
    reserve += 0.30 * max(0.0, losses_next)
    reserve += 0.18 * clamp(wind_risk, 0.0, 1.5) * max(1.0, generation_next)
    reserve += 0.12 * clamp(network_risk, 0.0, 1.5) * total_capacity
    reserve += 0.08 * clamp(1.0 - market_fill, 0.0, 1.0) * total_capacity
    reserve += 0.10 * anti_dump_pressure * total_capacity
    reserve += 0.12 * max(0.0, -safe_float(balance_now.get("balance", 0.0), 0.0))
    return clamp(reserve, MIN_RESERVE_SHARE * total_capacity, MAX_RESERVE_SHARE * total_capacity)


def next_tick_context(
    psm: Any,
    state: Dict[str, Any],
    balance_now: Dict[str, float],
    next_consumption: float,
) -> Dict[str, float]:
    rows = generation_rows(psm)
    update_generation_models(state, rows, psm)

    generation_ctx = predict_next_generation(psm, state, rows)
    network_ctx = network_pressure_risk(psm)

    update_loss_model(state, balance_now)
    losses_next = predict_next_losses(
        state,
        balance_now,
        generation_next=generation_ctx["total_gen_pred"],
        consumption_next=next_consumption,
        network_risk=network_ctx["risk"],
    )
    balance_next = generation_ctx["total_gen_pred"] - next_consumption - losses_next
    return {
        "generation_next": generation_ctx["total_gen_pred"],
        "losses_next": losses_next,
        "balance_next": balance_next,
        "wind_generation_next": generation_ctx["wind_gen_pred"],
        "wind_risk": generation_ctx["wind_risk"],
        "branch_risk": network_ctx["risk"],
        "branch_pressure_ratio": network_ctx["pressure_ratio"],
        "branch_loss_concentration": network_ctx["loss_concentration"],
        "sun_forecast": generation_ctx["sun_forecast"],
        "wind_forecast": generation_ctx["wind_forecast"],
    }


def market_execution(psm: Any) -> Dict[str, float]:
    asked = 0.0
    contracted = 0.0
    ask_prices: List[float] = []
    contract_prices: List[float] = []

    for receipt in getattr(psm, "exchange", []):
        asked_amount = safe_float(getattr(receipt, "askedAmount", 0.0), 0.0)
        if asked_amount >= 0.0:
            continue
        asked_price = safe_float(getattr(receipt, "askedPrice", 0.0), 0.0)
        contracted_amount = safe_float(getattr(receipt, "contractedAmount", 0.0), 0.0)
        contracted_price = safe_float(getattr(receipt, "contractedPrice", 0.0), 0.0)

        asked += abs(asked_amount)
        contracted += abs(contracted_amount)
        if asked_price > 0.0:
            ask_prices.append(asked_price)
        if abs(contracted_amount) > 0.0 and contracted_price > 0.0:
            contract_prices.append(contracted_price)

    fill = contracted / asked if asked > 1e-9 else 0.75
    return {
        "fill": clamp(fill, 0.0, 1.0),
        "ask_avg": mean(ask_prices, 0.0),
        "contract_avg": mean(contract_prices, 0.0),
    }


def market_prices(psm: Any) -> Dict[str, float]:
    cfg = getattr(psm, "config", {})
    step = max(0.01, safe_float(cfg.get("exchangeConsumerPriceStep", 0.2), 0.2))
    floor = max(MIN_PRICE, safe_float(cfg.get("exchangeExternalSell", MIN_PRICE), MIN_PRICE))
    cap = MAX_PRICE
    ext_buy = safe_float(cfg.get("exchangeExternalBuy", 10.0), 10.0)
    best_sell_ceiling = clamp(ext_buy, floor + step, cap)
    exec_stats = market_execution(psm)

    log_prices: List[float] = []
    for value in list(getattr(psm, "exchangeLog", []))[-10:]:
        price = safe_float(value, 0.0)
        if price > 0.0:
            log_prices.append(price)

    if log_prices:
        reference = sum(log_prices) / len(log_prices)
    elif exec_stats["contract_avg"] > 0.0:
        reference = exec_stats["contract_avg"]
    else:
        reference = ext_buy - step

    if log_prices and exec_stats["contract_avg"] > 0.0:
        reference = 0.6 * reference + 0.4 * exec_stats["contract_avg"]

    base = reference - step
    if exec_stats["fill"] < 0.35:
        base -= 2.0 * step
    elif exec_stats["fill"] < 0.65:
        base -= step
    elif exec_stats["fill"] > 0.90:
        base += 0.5 * step

    if exec_stats["ask_avg"] > 0.0 and exec_stats["fill"] < 0.5:
        base = min(base, exec_stats["ask_avg"] - step)

    base = round_price(base, step=step, low=floor + step, high=best_sell_ceiling)
    low = round_price(base - step, step=step, low=floor + step, high=base)
    high = round_price(base + step, step=step, low=base, high=best_sell_ceiling)

    return {
        "step": step,
        "floor": floor,
        "cap": cap,
        "reference": reference,
        "fill": exec_stats["fill"],
        "low": low,
        "base": base,
        "high": high,
    }


def split_charge(storages: List[Dict[str, Any]], total: float, per_cell_rate: float, cell_capacity: float) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = max(0.0, total)
    for row in sorted(storages, key=lambda item: item["soc"]):
        if remaining < MIN_ORDER_VOLUME:
            break
        room = max(0.0, cell_capacity - row["soc"])
        amount = round_vol(min(remaining, per_cell_rate, room))
        if amount >= MIN_ORDER_VOLUME:
            orders.append((row["id"], amount))
            remaining -= amount
    return orders


def split_discharge(
    storages: List[Dict[str, Any]],
    total: float,
    per_cell_rate: float,
    cell_capacity: float,
    floor_total: float,
) -> List[Tuple[str, float]]:
    orders: List[Tuple[str, float]] = []
    remaining = max(0.0, total)
    if not storages:
        return orders
    floor_per_cell = clamp(floor_total / len(storages), 0.0, cell_capacity)
    for row in sorted(storages, key=lambda item: item["soc"], reverse=True):
        if remaining < MIN_ORDER_VOLUME:
            break
        available = max(0.0, row["soc"] - floor_per_cell)
        amount = round_vol(min(remaining, per_cell_rate, available))
        if amount >= MIN_ORDER_VOLUME:
            orders.append((row["id"], amount))
            remaining -= amount
    return orders


def build_sell_orders(volume: float, low_price: float, base_price: float, high_price: float, fill: float) -> List[Tuple[float, float]]:
    volume = round_vol(volume)
    if volume < MIN_ORDER_VOLUME:
        return []

    if volume < 3.0:
        shares = [1.0]
        prices = [low_price]
    elif volume < 8.0:
        shares = [0.85, 0.15] if fill < 0.5 else [0.75, 0.25]
        prices = [low_price, base_price]
    else:
        shares = [0.75, 0.20, 0.05] if fill < 0.5 else [0.60, 0.30, 0.10]
        prices = [low_price, base_price, high_price]

    allocations = [round_vol(volume * share) for share in shares]
    allocations[0] = round_vol(allocations[0] + (volume - sum(allocations)))

    merged: Dict[float, float] = {}
    for part, price in zip(allocations, prices):
        if part < MIN_ORDER_VOLUME:
            continue
        merged[price] = round_vol(merged.get(price, 0.0) + part)

    return [(vol, price) for price, vol in sorted(merged.items()) if vol >= MIN_ORDER_VOLUME]


def controller(psm: Any) -> Dict[str, Any]:
    cfg = getattr(psm, "config", {})
    tick = int(getattr(psm, "tick", 0))
    state = load_state()

    storages = storage_rows(psm)
    cells = len(storages)
    cell_capacity = max(1.0, safe_float(cfg.get("cellCapacity", 120.0), 120.0))
    charge_rate = max(0.0, safe_float(cfg.get("cellChargeRate", 15.0), 15.0))
    discharge_rate = max(0.0, safe_float(cfg.get("cellDischargeRate", 20.0), 20.0))

    total_capacity = cells * cell_capacity
    total_soc = sum(clamp(row["soc"], 0.0, cell_capacity) for row in storages)

    balance_now = current_balance(psm)
    surplus = max(0.0, balance_now["balance"])
    deficit = max(0.0, -balance_now["balance"])
    prices = market_prices(psm)
    next_consumption = next_tick_consumption(psm)
    next_ctx = next_tick_context(psm, state, balance_now, next_consumption)
    anti_dump_cap = anti_dump_limit(state, tick)

    reserve_floor = 0.0
    if cells > 0:
        reserve_floor = compute_dynamic_reserve(
            total_capacity=total_capacity,
            balance_now=balance_now,
            next_consumption=next_consumption,
            generation_next=next_ctx["generation_next"],
            losses_next=next_ctx["losses_next"],
            wind_risk=next_ctx["wind_risk"],
            network_risk=next_ctx["branch_risk"],
            market_fill=prices["fill"],
            anti_dump_cap=anti_dump_cap,
        )
    reserve_floor = clamp(reserve_floor, 0.0, total_capacity)
    full_threshold = max(0.96 * total_capacity, reserve_floor + 0.06 * total_capacity)
    risk_score = clamp(
        0.45 * clamp(-next_ctx["balance_next"] / max(1.0, next_consumption), 0.0, 1.5)
        + 0.30 * next_ctx["wind_risk"]
        + 0.25 * next_ctx["branch_risk"],
        0.0,
        1.5,
    )

    charge_plan = 0.0
    discharge_for_deficit = 0.0
    discharge_for_market = 0.0
    sell_priority = 1.0

    if deficit >= MIN_ORDER_VOLUME and total_soc > reserve_floor:
        discharge_for_deficit = min(
            deficit,
            cells * discharge_rate,
            max(0.0, total_soc - reserve_floor),
        )
    elif surplus >= MIN_ORDER_VOLUME:
        if cells > 0:
            # Sell-first: default is to push most surplus to market,
            # and only then send the remaining part to storage.
            sell_priority = 0.95
            if prices["fill"] < 0.35:
                sell_priority = 0.55
            elif prices["fill"] < 0.60:
                sell_priority = 0.75
            elif prices["fill"] < 0.80:
                sell_priority = 0.90

            if prices["reference"] <= prices["floor"] + 2.0 * prices["step"]:
                sell_priority = min(sell_priority, 0.65)

            if next_ctx["balance_next"] < -2.0:
                sell_priority = min(sell_priority, 0.52)
            elif next_ctx["balance_next"] < -0.5:
                sell_priority = min(sell_priority, 0.68)
            elif risk_score > 0.9:
                sell_priority = min(sell_priority, 0.75)

            if next_ctx["wind_risk"] > 1.0:
                sell_priority = min(sell_priority, 0.70)
            if next_ctx["branch_risk"] > 0.9:
                sell_priority = min(sell_priority, 0.65)

            expected_surplus = max(0.0, next_ctx["balance_next"])
            if expected_surplus >= MIN_ORDER_VOLUME:
                anti_dump_pressure = clamp((expected_surplus - anti_dump_cap) / max(expected_surplus, 1.0), 0.0, 1.0)
                if anti_dump_pressure > 0.0:
                    sell_priority = min(sell_priority, 0.85 - 0.35 * anti_dump_pressure)

            sell_priority = clamp(sell_priority, 0.35, 1.0)
            planned_sell_from_surplus = surplus * sell_priority
            surplus_for_storage = max(0.0, surplus - planned_sell_from_surplus)
            prep_charge_bonus = clamp(0.20 + 0.45 * max(0.0, -next_ctx["balance_next"]) / max(next_consumption, 1.0), 0.0, 0.65)
            planned_charge_total = surplus_for_storage + prep_charge_bonus * surplus
            charge_plan = min(
                planned_charge_total,
                cells * charge_rate,
                max(0.0, total_capacity - total_soc),
            )
        else:
            sell_priority = 1.0

    market_window_ok = (
        prices["fill"] >= 0.65
        and next_ctx["balance_next"] >= -0.35
        and next_ctx["wind_risk"] <= 1.0
        and next_ctx["branch_risk"] <= 1.0
    )
    if deficit < MIN_ORDER_VOLUME and total_soc >= full_threshold and cells > 0 and market_window_ok:
        discharge_for_market = min(
            0.30 * cells * discharge_rate,
            max(0.0, total_soc - reserve_floor),
        )

    discharge_plan = discharge_for_deficit + discharge_for_market

    charge_orders = split_charge(storages, charge_plan, charge_rate, cell_capacity)
    discharge_orders = split_discharge(storages, discharge_plan, discharge_rate, cell_capacity, reserve_floor)

    real_charge = sum(amount for _, amount in charge_orders)
    real_discharge = sum(amount for _, amount in discharge_orders)

    market_discharge = 0.0
    if discharge_plan > 0.0 and discharge_for_market > 0.0:
        market_discharge = real_discharge * (discharge_for_market / discharge_plan)

    projected_soc = clamp(total_soc + real_charge - real_discharge, 0.0, total_capacity)
    reserve_gap = max(0.0, reserve_floor - projected_soc)
    sell_volume = max(0.0, surplus - real_charge + market_discharge - reserve_gap)
    if prices["fill"] < 0.35:
        sell_volume *= 0.72
    elif prices["fill"] < 0.55:
        sell_volume *= 0.88
    sell_volume = min(sell_volume, anti_dump_cap)

    sell_orders = build_sell_orders(
        sell_volume,
        prices["low"],
        prices["base"],
        prices["high"],
        prices["fill"],
    )

    for storage_id, amount in charge_orders:
        psm.orders.charge(storage_id, amount)
    for storage_id, amount in discharge_orders:
        psm.orders.discharge(storage_id, amount)
    for volume, price in sell_orders:
        psm.orders.sell(volume, price)

    state_section(state, "last_tick").update(
        {
            "balance_next": round(next_ctx["balance_next"], 6),
            "generation_next": round(next_ctx["generation_next"], 6),
            "losses_next": round(next_ctx["losses_next"], 6),
            "wind_risk": round(next_ctx["wind_risk"], 6),
            "branch_risk": round(next_ctx["branch_risk"], 6),
            "market_fill": round(prices["fill"], 6),
            "reserve_floor": round(reserve_floor, 6),
            "anti_dump_cap": round(anti_dump_cap, 6),
        }
    )
    save_state(state, tick, balance_now["useful"])

    mode = "hold"
    if discharge_for_deficit >= MIN_ORDER_VOLUME:
        mode = "discharge_deficit"
    elif charge_plan >= MIN_ORDER_VOLUME:
        mode = "charge_surplus"
    if discharge_for_market >= MIN_ORDER_VOLUME:
        mode = "sell_full_battery" if mode == "hold" else f"{mode}+sell_full_battery"

    return {
        "tick": tick,
        "mode": mode,
        "balance_now": round(balance_now["balance"], 6),
        "next_consumption": round(next_consumption, 6),
        "next_generation_pred": round(next_ctx["generation_next"], 6),
        "next_losses_pred": round(next_ctx["losses_next"], 6),
        "next_balance_pred": round(next_ctx["balance_next"], 6),
        "soc": round(total_soc, 6),
        "capacity": round(total_capacity, 6),
        "reserve_floor": round(reserve_floor, 6),
        "charge": round(real_charge, 6),
        "discharge": round(real_discharge, 6),
        "market_ref": round(prices["reference"], 6),
        "market_fill": round(prices["fill"], 6),
        "wind_risk": round(next_ctx["wind_risk"], 6),
        "branch_risk": round(next_ctx["branch_risk"], 6),
        "risk_score": round(risk_score, 6),
        "anti_dump_cap": round(anti_dump_cap, 6),
        "sell_priority": round(sell_priority, 3),
        "sell_volume": round(sum(v for v, _ in sell_orders), 6),
        "sell_orders": sell_orders,
    }


def main() -> None:
    psm = ips.init()
    try:
        report = controller(psm)
        save_report(report)
        print(json.dumps(report, ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "tick": int(getattr(psm, "tick", 0)),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=False,
            )
        )
    finally:
        if hasattr(psm, "save_and_exit"):
            psm.save_and_exit()


if __name__ == "__main__":
    main()
