# IES controller README — unified strategy for `main.py` and `short_main.py`

This README is written for Codex / GPT-5.4 as an **engineering map of the current control strategy**.

Its purpose is not just to describe functions, but to explain:

- what the controller is trying to achieve;
- what behavior must stay invariant across edits;
- how `main.py` and `short_main.py` relate to each other;
- what the **single intended strategy** is for:
  - battery control;
  - market selling;
  - their interaction.

This file should be read **before** editing either `main.py` or `short_main.py`.

---

# 1. Controller family

The repository currently contains **two controller variants**:

- `main.py` — the **primary / full controller**
- `short_main.py` — the **compact / reduced controller**

They are not identical in implementation, but they should follow the **same strategic doctrine**.

## 1.1 `main.py`
`main.py` is the richer controller.

It includes:
- persistent state;
- forecast ingestion and harmonization;
- empirical learning from past ticks;
- topology analysis;
- market history and fill-rate tracking;
- dynamic storage policy;
- conservative sell-volume calculation;
- logging / debugging / derived metrics.

Use `main.py` as the **reference implementation of intent** when possible.

## 1.2 `short_main.py`
`short_main.py` is the simplified controller.

It keeps:
- current balance estimation;
- short lookahead forecast;
- dynamic battery targets;
- market reference;
- sell ladder generation.

It is intentionally much smaller and more heuristic, but it should still obey the **same priorities** as `main.py`.

---

# 2. The single strategy that both files must follow

This is the most important section.

## 2.1 Primary rule
**Internal balance is always more important than market monetization.**

That means:

1. first protect internal system balance;
2. then decide battery charge / discharge;
3. only then decide what energy is truly safe to sell;
4. only then place market sell orders.

If a future edit changes this order, it is probably wrong.

## 2.2 Battery-first, market-second
The controller must never behave as if:
- market selling is the main objective;
- battery is just an optional add-on.

Correct philosophy:
- battery is a balancing and shifting tool;
- market selling consumes only safe residual energy.

## 2.3 Deficit is handled before profit
If the system is in a real current deficit:
- battery discharge should support the system first;
- battery should not charge;
- battery should not market-discharge for profit before internal balance is safe.

## 2.4 Surplus is split in this order
If the system has a real current surplus:
1. battery may absorb energy if it is below target or the future is risky;
2. only remaining safe surplus may be sold;
3. market discharge from battery is a secondary, optional action.

---

# 3. Rules-level constraints that the code must respect

These constraints come from the game rules and must remain consistent in both files.

## 3.1 Battery
Battery parameters:
- capacity = `120 MWh-equivalent per tick-cell`
- max charge per tick per cell = `15`
- max discharge per tick per cell = `20`
- orders act for one tick only
- multiple same-tick orders are cumulative

## 3.2 Market
Market constraints:
- only **useful energy** should be sold;
- up to 100 sell orders may be placed;
- prices must remain inside legal limits;
- underdelivery is dangerous;
- unsold energy may go to the external side / GP at poor economics;
- anti-dumping limit must be respected:
  - `max asked amount <= 1.2 * previous useful energy + 10`

## 3.3 Consequence for strategy
This means the controller must be:
- conservative on sellable volume;
- moderately adaptive on price;
- strict about not selling energy needed for internal balance.

---

# 4. Unified battery strategy

This section defines the **single intended battery doctrine** for both `main.py` and `short_main.py`.

## 4.1 Battery has four modes conceptually

### A. `deficit_support`
Use battery to reduce real current deficit.

This is the **highest-priority mode**.

Expected behavior:
- triggered when current balance is negative enough;
- discharge allowed down to a safe floor;
- battery should help reduce import / deficit;
- no charging;
- market discharge must not outrank this mode.

### B. `surplus_capture`
Use battery to absorb current surplus when that energy is more valuable later than now.

Expected behavior:
- triggered when current balance is positive;
- battery below target or risk ahead is high;
- market selling reduced if battery needs the energy.

### C. `market_support`
Use battery as an **additional market amplifier**, but only if internal balance is already safe.

Expected behavior:
- only allowed when:
  - current deficit is absent;
  - battery is above safe reserve;
  - near-future risk is acceptable;
  - market quality is good enough.
- should be limited and never become the dominant reason for discharge.

### D. `hold`
Do nothing.

This mode is valid only if:
- there is no strong reason to charge;
- no strong reason to discharge;
- battery reserve state is already acceptable.

`hold` must not become a lazy default when the system is clearly in deficit or clearly has harvestable surplus.

---

## 4.2 Battery priorities in order

The correct priority stack is:

1. protect system from current deficit;
2. preserve enough reserve for near-future risk;
3. capture surplus if useful;
4. support market if still safe;
5. otherwise hold.

This order should remain invariant.

---

## 4.3 Meaning of SOC thresholds

Both controllers may use different variable names, but the semantics should stay aligned.

### `emergency_floor_soc`
Almost untouchable minimum.

Purpose:
- prevent pathological full depletion;
- preserve minimum survivability.

This floor should be used only in genuinely stressed situations.

### `working_floor_soc`
Normal lower operating floor.

Purpose:
- battery may safely discharge down to this region when supporting real deficit.

This should be the main operational floor during normal deficit handling.

### `target_soc`
Desired reserve level for the near future.

Purpose:
- store enough energy to survive likely upcoming deficits;
- adapt to predicted risk.

### `high_risk_target_soc` / `prep_soc` / `protected_soc`
These are stronger reserve targets in `main.py`.

Purpose:
- keep more energy when future risk is elevated;
- reflect topology / losses / weak market / expected solar drop / wind instability.

Important:
these values must **not freeze the battery forever** if real current deficit is already happening.

---

## 4.4 What battery should do in key situations

### Situation 1 — current deficit, high SOC
Expected:
- discharge into the system;
- reduce import / deficit;
- do not charge;
- do not sell for market first.

### Situation 2 — current surplus, low SOC
Expected:
- charge battery;
- do not over-sell;
- preserve energy for future risk.

### Situation 3 — current surplus, battery near target, market strong
Expected:
- sell residual safe energy;
- optional limited market discharge is acceptable.

### Situation 4 — current deficit, battery full, market attractive
Expected:
- still protect internal balance first;
- only safe post-balance residual may be sold.

### Situation 5 — endgame, high SOC
Expected:
- battery may discharge more aggressively;
- ending with large useless stored energy is bad.

---

# 5. Unified market strategy

This is the single intended doctrine for selling energy.

## 5.1 Never sell “optimism”
Market sells must be based on **safe deliverable energy**, not on idealized generation.

Correct mental model:
- start from useful or physically safe surplus;
- subtract battery charging needs;
- subtract future-deficit protection;
- subtract uncertainty / reserve;
- only then sell the remainder.

## 5.2 Volume is more important than price correctness
The controller should first decide:
- **is there any safe volume to sell?**

Only after that:
- decide price ladder.

Overstating sellable volume is worse than mild underpricing.

## 5.3 Price is adaptive, but bounded
Price logic should use:
- recent market prices;
- fill ratio;
- current ask/contract evidence;
- external buy cap / GP anchor;
- market realism.

Strong market can justify:
- slightly more aggressive ladder;
- possibly some controlled battery-to-market discharge.

Weak market should push toward:
- lower prices;
- lower sell volume;
- stronger preservation of energy in battery.

## 5.4 Battery-to-market discharge is secondary
Battery-to-market discharge is allowed only if:
- there is no real internal deficit;
- battery is clearly above safe operating reserve;
- future risk is acceptable;
- market quality is supportive.

If an edit makes battery-to-market discharge easy or default, that is probably a regression.

---

# 6. The single coupling rule: battery + market

This is the core invariant for both files.

## 6.1 Mandatory order of decision-making
The controller must conceptually do:

1. evaluate current balance;
2. evaluate future risk;
3. decide battery charge/discharge for system safety;
4. compute safe exportable energy;
5. build sell ladder;
6. place orders.

This ordering is mandatory.

## 6.2 Forbidden logical inversion
The controller must **not** do this:

- decide market sell first,
- then try to rescue balance with the battery.

That is strategically backwards.

## 6.3 Correct question
The controller should ask:

> “After battery safety action, how much safe energy is left for the market?”

not:

> “How much can we sell, and can battery somehow support it?”

---

# 7. `main.py` architecture

This section explains the major intent of `main.py` so Codex can edit it safely.

## 7.1 High-level pipeline
`main.py` roughly does:

1. load persistent state;
2. normalize configuration;
3. extract objects / networks / exchange rows;
4. build topology insight;
5. build forecast bundle;
6. update startup/runtime context;
7. build forecast profile and future window;
8. compute theoretical current metrics;
9. compute current useful energy and balance;
10. analyze market and update market history;
11. compute storage plan;
12. compute safe sell volume;
13. build ladder;
14. place orders;
15. log metrics and update learning state.

## 7.2 Why `main.py` exists
`main.py` is meant to be:
- more empirical;
- more robust to noisy data;
- more aware of topology and forecast quality;
- more adaptive to market fill and uncertainty.

## 7.3 Strategic interpretation of `main.py`
`main.py` is not just “bigger”.
It is intended to implement these ideas:

- battery targets should depend on:
  - forecast deficits;
  - solar drop risk;
  - wind instability;
  - loss pressure;
  - fill-rate;
  - anti-dumping headroom;
  - topology stress;
- safe sell volume should be explicitly limited by:
  - useful energy;
  - reserve;
  - uncertainty;
  - fill-rate;
  - price realism;
  - topology warnings.

## 7.4 Important design intentions in `main.py`
When editing `main.py`, preserve these intentions:

1. **stateful adaptation matters**
   - market history and model learning are deliberate features.

2. **topology can reduce market aggressiveness**
   - high losses and stressed topology should reduce optimism.

3. **forecast profile matters**
   - not every tick should be treated identically;
   - solar windows, risk windows, mixed peak windows matter.

4. **battery policy is richer than simple target/floor**
   - `prep_soc`, `protected_soc`, `working_floor_soc`, `emergency_floor_soc`, `allow_market_discharge` are trying to represent different reserve semantics.

5. **safe sell volume is explicit**
   - this is the correct architecture direction and should be preserved.

---

# 8. `short_main.py` architecture

## 8.1 High-level pipeline
`short_main.py` roughly does:

1. classify objects;
2. compute current balance;
3. compute short 4-tick forecast;
4. estimate market reference;
5. decide storage actions;
6. build a small sell ladder;
7. place orders.

## 8.2 Why `short_main.py` exists
It is a compact controller for:
- fast experiments;
- reduced complexity;
- simple reasoning;
- smaller code surface.

## 8.3 What must stay aligned with `main.py`
Even though it is smaller, it must still obey the same strategy:

- deficit support before market;
- surplus capture before aggressive selling;
- market discharge only as secondary action;
- conservative sell volume.

## 8.4 Expected simplifications
It is acceptable that `short_main.py` is weaker in:
- topology awareness;
- anti-dumping enforcement;
- empirical loss modeling;
- persistent state;
- multi-signal reserve logic.

But it should still be **directionally consistent** with the full controller.

---

# 9. Shared invariants that must not be broken

These apply to **both** files.

1. Do not charge battery during real current deficit.
2. Do not prioritize market discharge over internal balance protection.
3. Do not drain battery to zero without explicit reason.
4. Do not assume all current generation is safe to sell.
5. Do not ignore uncertainty when building sell volume.
6. Do not create physically impossible charge/discharge orders.
7. Do not rely on price only; volume safety comes first.
8. Do not forget that anti-dumping must constrain market behavior.
9. Do not allow battery logic and market logic to drift into contradictory objectives.

---

# 10. Known current weaknesses

Codex should understand these are not intentional virtues.

## 10.1 `short_main.py` is intentionally shallow
Weaknesses may include:
- rough loss modeling;
- shallow market model;
- shallow battery reserve logic;
- no deep topology use;
- no explicit anti-dumping enforcement.

## 10.2 `main.py` is richer, but more fragile
Potential risk areas:
- too much conservatism in battery floors / prep targets;
- accidental “hold while importing” behavior if reserve protection dominates too much;
- complexity around market realism vs safe sell volume;
- forecast/model interactions becoming too opaque.

## 10.3 README priority
When `main.py` and `short_main.py` diverge, the preferred question is:

> Which version better matches the unified strategy in this README?

not:

> Which version is shorter or easier to patch?

---

# 11. Canonical decision doctrine for future edits

If Codex edits either file, the target behavior should be:

## Step 1 — measure
Estimate:
- current physical balance;
- useful energy;
- forecast risk;
- battery state;
- market quality.

## Step 2 — protect
Use battery to prevent or reduce deficit before chasing revenue.

## Step 3 — preserve
Keep enough reserve for likely near-future stress.

## Step 4 — monetize
Sell only the energy that remains safely exportable after Steps 2–3.

## Step 5 — explain
Keep debug / metrics sufficient to explain why:
- battery charged or discharged;
- market was active or not;
- reserve was high or low.

---

# 12. Canonical behavior examples

These examples are intentionally redundant, because they define expected intent clearly.

## Case A — deficit now, battery has energy
Correct:
- discharge toward working floor;
- reduce import;
- market sells off or strongly reduced.

Incorrect:
- hold battery with high SOC while importing;
- sell in market before balance is safe.

## Case B — surplus now, weak market, battery below target
Correct:
- charge battery;
- sell little or nothing.

Incorrect:
- dump most surplus to market while battery stays underfilled.

## Case C — surplus now, strong market, battery already well-filled
Correct:
- sell a meaningful fraction of safe surplus;
- optional limited battery market discharge.

Incorrect:
- aggressively empty battery without checking future risk.

## Case D — endgame, battery still full
Correct:
- use battery more aggressively;
- avoid finishing with obviously wasted stored energy.

Incorrect:
- preserve large SOC until the game ends.

## Case E — high uncertainty / weak fill / bad topology
Correct:
- sell volume reduced;
- reserve increased;
- battery kept more defensive.

Incorrect:
- use optimistic sell volume as if all energy is equally safe.

---

# 13. Practical mapping between files

This section helps Codex move ideas between the two files.

## Battery logic mapping
- `short_main.py`
  - `choose_storage_targets(...)`
  - `decide_storage_actions(...)`

- `main.py`
  - `compute_target_soc(...)`
  - `storage_policy(...)`

Meaning:
- changes to unified reserve philosophy should usually be mirrored conceptually across these pairs.

## Market logic mapping
- `short_main.py`
  - `market_reference(...)`
  - `build_sell_ladder(...)`

- `main.py`
  - market history / market context
  - `compute_safe_sell_volume(...)`
  - `build_ladder(...)`

Meaning:
- `main.py` should stay richer;
- `short_main.py` should remain consistent in direction.

## Main pipeline mapping
- `short_main.py`
  - lightweight one-pass control

- `main.py`
  - stateful, logged, adaptive control

---

# 14. What Codex should preserve

Preserve:
- battery-before-market reasoning;
- safe-volume-first selling;
- endgame discharge logic;
- physically valid per-cell order distribution;
- robust handling of missing/partial data;
- useful debug information.

---

# 15. What Codex may improve

Safe improvement areas:

1. explicit anti-dumping enforcement in all variants;
2. stronger chronic-deficit battery support;
3. better useful-energy estimation;
4. richer ladder pricing;
5. better topology-aware sell reduction;
6. better chronic-import detection;
7. more explainable battery mode reasons.

---

# 16. What Codex must treat as regressions

The following changes are regressions unless explicitly justified:

- battery charging on real deficit;
- battery market-discharge before internal deficit is safe;
- sell volume computed before storage action;
- holding high SOC while repeatedly importing with no strong future reason;
- ignoring anti-dumping;
- removing reserve logic without replacement;
- treating market price as more important than deliverability.

---

# 17. One-sentence summary

Both `main.py` and `short_main.py` should behave as **conservative balance-first controllers**:

**protect the system first, store useful surplus second, sell only safe residual energy third, and use battery-for-market only as a controlled extra.**
