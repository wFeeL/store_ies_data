import json
import traceback
from typing import Any, Dict

import ips
import main as strategy


def controller(psm: Any) -> Dict[str, Any]:
    state = strategy.load_state()
    tick = strategy.get_tick(psm)
    if tick == 0:
        state = strategy.default_state()

    game_length = strategy.get_game_length(psm)
    cfg = strategy.normalize_cfg(strategy.get_config_dict(psm))
    object_rows = strategy.extract_object_rows(psm)
    network_rows = strategy.extract_network_rows(psm)
    exchange_rows = [strategy.exchange_receipt_data(x) for x in strategy.get_exchange_list(psm)]

    total_generated, total_consumed, total_external, total_losses = strategy.get_total_power_tuple(psm)
    obj_agg = strategy.aggregate_objects(object_rows)
    topology = strategy.analyze_topology(object_rows, network_rows, total_generated)
    weather = {
        'wind': strategy.get_weather_now(psm, 'wind'),
        'sun': strategy.get_weather_now(psm, 'sun'),
    }
    forecast_bundle = strategy.get_forecast_bundle(psm, game_length=game_length, cfg=cfg, object_rows=object_rows)

    state['cfg_runtime'] = cfg
    state['weather_runtime'] = weather
    strategy.refresh_static_runtime_context(state, object_rows)
    strategy.apply_startup_observation(
        state,
        object_rows,
        forecast_bundle,
        tick,
        total_consumed=total_consumed,
    )

    forecast_profile = strategy.build_forecast_profile(state, forecast_bundle, object_rows, game_length)
    profile_ctx = strategy.forecast_profile_context(forecast_profile, tick, horizon=max(12, strategy.LOOKAHEAD * 2))
    future = strategy.forecast_window(state, object_rows, forecast_bundle, tick, game_length, strategy.LOOKAHEAD)
    current_theoretical = strategy.current_theoretical_metrics(state, object_rows, weather, forecast_bundle, tick, cfg)

    useful_now = max(0.0, strategy.compute_useful_energy(total_generated, total_consumed, total_losses))
    balance_now = strategy.compute_balance_energy(total_generated, total_consumed, total_losses)

    market_stats = strategy.analyze_exchange(exchange_rows)
    sell_avg_price = market_stats.get('sell_avg_contracted_price')
    buy_ref = market_stats.get('buy_avg_contracted_price') or market_stats.get('buy_avg_asked_price')
    fill_ratio_now = market_stats.get('sell_fill_ratio')
    exch_log = strategy.get_exchange_log(psm)

    if sell_avg_price is not None:
        state['market_ref'] = 0.76 * strategy.safe_float(state.get('market_ref', 4.8), 4.8) + 0.24 * sell_avg_price
    elif buy_ref is not None:
        prev_ref = strategy.safe_float(state.get('market_ref', 4.8), 4.8)
        capped_buy_ref = min(strategy.safe_float(buy_ref, prev_ref), prev_ref + 1.2)
        state['market_ref'] = 0.88 * prev_ref + 0.12 * capped_buy_ref
    elif tick > 0 and tick - 1 < len(exch_log):
        last_log_price = strategy.safe_float(exch_log[tick - 1], state.get('market_ref', 4.8))
        state['market_ref'] = 0.88 * strategy.safe_float(state.get('market_ref', 4.8), 4.8) + 0.12 * last_log_price

    if fill_ratio_now is not None:
        state['fill_ratio_ewma'] = 0.76 * strategy.safe_float(state.get('fill_ratio_ewma', 0.84), 0.84) + 0.24 * fill_ratio_now

    strategy.update_market_history(state, tick, market_stats)
    market_ctx = strategy.build_market_context(state, market_stats)
    anti_dump_cap_preview = strategy.compute_offer_cap(state, cfg, tick, useful_now)
    market_ctx['anti_dump_cap'] = anti_dump_cap_preview
    market_ctx['anti_dump_headroom'] = max(0.0, anti_dump_cap_preview - strategy.safe_float(state.get('last_sell_volume', 0.0), 0.0))

    decision_loss_ratio = strategy.safe_float(state.get('loss_ratio_ewma', 0.18), 0.18)
    charge_orders, discharge_orders, battery_dbg = strategy.storage_policy(
        state,
        cfg,
        obj_agg['storages'],
        balance_now,
        useful_now,
        future,
        strategy.safe_float(market_ctx.get('recent_fill_ratio', state.get('fill_ratio_ewma', 0.84)), 0.84),
        tick,
        game_length,
        loss_ratio=decision_loss_ratio,
        profile_ctx=profile_ctx,
        market_ctx=market_ctx,
    )

    startup_mode = strategy.startup_active(state, tick)
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

    offer_cap = strategy.compute_offer_cap(state, cfg, tick, marketable_useful_now)
    reserve = strategy.compute_reserve(state, future, object_rows, total_losses, marketable_useful_now, profile_ctx=profile_ctx)
    market_ctx['anti_dump_cap'] = offer_cap
    market_ctx['anti_dump_headroom'] = max(0.0, offer_cap - strategy.safe_float(state.get('last_sell_volume', 0.0), 0.0))

    sell_volume = strategy.compute_safe_sell_volume(
        state,
        object_rows,
        marketable_useful_now,
        offer_cap,
        reserve,
        balance_now,
        topology,
        market_ctx,
    )
    if startup_mode and battery_dbg.get('signal', 0.0) <= reserve + 0.5:
        sell_volume = 0.0
    elif (
        marketable_useful_now >= strategy.MIN_ORDER_VOLUME
        and sell_volume <= 0.0
        and market_ctx.get('good_fill')
        and strategy.safe_float(market_ctx.get('price_realism', 1.0), 1.0) >= 0.85
    ):
        sell_volume = strategy.round_vol(
            min(offer_cap, marketable_useful_now, max(strategy.MIN_ORDER_VOLUME, 0.48 * marketable_useful_now))
        )

    storage_excess = battery_dbg['total_soc'] > battery_dbg.get('prep_soc', battery_dbg['target_soc']) + 0.08 * len(obj_agg['storages']) * strategy.safe_float(cfg['cellCapacity'], 120.0)
    ladder = strategy.build_ladder(
        sell_volume,
        strategy.safe_float(state.get('market_ref', 4.8), 4.8),
        strategy.safe_float(market_ctx.get('recent_fill_ratio', state.get('fill_ratio_ewma', 0.84)), 0.84),
        strategy.safe_int(cfg['exchangeMaxTickets'], 100),
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

    strategy.apply_post_tick_learning(
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
    strategy.save_state(state)

    return {
        'tick': tick,
        'game_length': game_length,
        'forecast_source': forecast_bundle.get('_meta', {}).get('source'),
        'forecast_rows': forecast_bundle.get('_meta', {}).get('rows'),
        'wind_now': round(weather['wind'], 6),
        'sun_now': round(weather['sun'], 6),
        'load_forecast_now': round(current_theoretical['load_forecast_now'], 6),
        'load_model_now': round(current_theoretical['load_model_now'], 6),
        'total_generated': round(total_generated, 6),
        'total_consumed': round(total_consumed, 6),
        'total_external': round(total_external, 6),
        'total_losses': round(total_losses, 6),
        'physical_balance_now': round(balance_now, 6),
        'useful_supply_now': round(useful_now, 6),
        'marketable_useful_now': round(marketable_useful_now, 6),
        'sell_volume': round(sell_volume, 6),
        'charge_total': round(battery_dbg['charge_total'], 6),
        'discharge_total': round(battery_dbg['discharge_total'], 6),
        'target_soc': round(battery_dbg['target_soc'], 6),
        'prep_soc': round(strategy.safe_float(battery_dbg.get('prep_soc', battery_dbg['target_soc']), battery_dbg['target_soc']), 6),
        'total_soc': round(battery_dbg['total_soc'], 6),
        'storage_mode': battery_dbg.get('mode'),
        'storage_signal': round(strategy.safe_float(battery_dbg.get('signal', 0.0), 0.0), 6),
        'startup_load_scale': round(strategy.startup_scale(state, tick), 6),
        'market_ref': round(strategy.safe_float(state.get('market_ref', 4.8), 4.8), 6),
        'market_price_realism': round(strategy.safe_float(market_ctx.get('price_realism', 1.0), 1.0), 6),
        'topology_warnings': topology.get('warnings', []),
        'ladder': ladder,
    }


def main() -> None:
    psm = ips.init()
    try:
        summary = controller(psm)
        print(json.dumps(summary, ensure_ascii=False))
    except Exception as exc:
        err = {
            'tick': strategy.get_tick(psm),
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }
        print(json.dumps(err, ensure_ascii=False))
    finally:
        if hasattr(psm, 'save_and_exit'):
            psm.save_and_exit()


if __name__ == '__main__':
    main()
