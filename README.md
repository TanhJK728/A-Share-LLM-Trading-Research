<p align="center">
  <img src="assets/hero.svg" alt="Language. Expectations. Markets. — Time-aware NLP for Chinese financial disclosures" width="100%">
</p>

# A-Share NLP & Event Research

**When does financial language add predictive information beyond market state and information coverage?**

This project studies Chinese financial text through character-level representations, controlled prediction experiments, and time-aware event alignment. The research progresses from broad headline-content tests to a sharper question: how do fundamental news, surprise, and the market's prior response interact?

The current evidence combines a single-stock headline pilot with a five-stock disclosure-event study. The headline pilot exposes the importance of information-count controls; the event study shows how overlapping return windows can change a conclusion. Together, they motivate richer, evidence-linked semantic modeling.

<p align="center">
  <a href="#research-design">Research design</a> ·
  <a href="#mathematical-methods">Mathematical methods</a> ·
  <a href="#empirical-findings">Empirical findings</a> ·
  <a href="#next-surprise--market-state">Surprise × market state</a> ·
  <a href="#reproduce-the-figures">Reproduce</a>
</p>

| Research snapshot | Scope |
| :--- | :--- |
| **Latest experiment** | Stage 2.4 · frozen October 4, 2026 |
| **Temporal design** | Train: 2018–2025 · development validation: 2026, before October 4 |
| **Event evidence** | 306 unique events across 5 stocks · 25 validation events |
| **Completed comparison** | 7 non-LLM baselines × 3 horizons: 5, 20, and 60 sessions |
| **Semantic extension** | Evidence-linked, 15-field LLM schema prepared · inference pending |

## Research design

Stage 1 asks whether headline content improves daily prediction after controlling for the presence and number of titles. Stage 2.4 studies official financial disclosures as stock-specific events, with matched model comparisons and explicitly aligned entry and exit times.

![Research pipeline: provenance and timing, text and state representations, matched models, and multi-horizon evaluation. A dashed branch identifies the prepared LLM extension.](assets/research-pipeline.svg)

- **Control the information set.** Compare price/volume features, event metadata, and text on the same cohort. Stage 1 explicitly controls headline counts and presence; Stage 2.4 includes event metadata controls.
- **Fit representations on the past.** Vocabulary, IDF, numerical preprocessing, and score thresholds are learned from training data. Labels crossing the training boundary are excluded from fitting.
- **Inspect dependence.** Report every registered horizon, observed-outcome counts, and a same-stock non-overlap sensitivity alongside pooled event metrics.

Document hashes, version fields, source dates, and timing assumptions accompany the local artifacts. Date-only disclosures use conservative availability bounds. Historical first availability and original-body vintage remain assumptions requiring further source verification.

## Mathematical methods

### 1. Character-level text representation

Chinese text is represented with character bigrams, a fixed vocabulary of at most 512 features, sublinear term frequencies, and smoothed IDF. For a bigram with count $c_{dj}>0$ in document group $d$:

```math
u_{dj}=(1+\log c_{dj})\left(1+\log\frac{1+N_{\mathrm{train}}}{1+\mathrm{df}_{j,\mathrm{train}}}\right),
\qquad x_d=\frac{u_d}{\lVert u_d\rVert_2}.
```

Absent bigrams receive zero weight; an empty group remains a zero vector. This provides an interpretable lexical baseline for testing future semantic representations.

### 2. Matched feature ablations

Let $\phi_e$ contain a registered combination of market-state features $m_e$, event metadata $z_e$, and text features $x_e$. Each combination uses the same fixed ridge objective:

```math
(\hat b,\hat\beta)=\underset{b,\beta}{\arg\min}\;
\sum_{e\in\mathcal T}\left(y_{e,H}-b-\phi_e^\top\beta\right)^2
+10\lVert\beta\rVert_2^2.
```

The intercept is unpenalized. The full control combines $[m_e,z_e,x_e]$; comparison models remove feature families without tuning on the 2026 rankings.

### 3. Event-aligned targets

For Stage 2.4, entry is the first eligible session open after the assumed information-availability bound plus a five-minute processing allowance. The $H$-session label includes the entry session:

```math
y_{e,H}=
\left(\frac{P^{C}_{s,i+H-1}}{P^{O}_{s,i}}-1\right)
-\left(\frac{P^{C}_{B,i+H-1}}{P^{O}_{B,i}}-1\right),
\qquad H\in\{5,20,60\}.
```

Here $s$ is the event stock and $B$ is CSI 300. Prediction quality is summarized by **pooled stock-event Spearman Rank IC**, with a second evaluation after greedily removing overlapping holding windows for each stock. The five-stock CSI 300 branch is provisional while historical industry benchmarks are completed.

## Empirical findings

### Stage 1 · Headline content needs an information-count control

The pilot uses **Ping An Bank**, training in **2018–2019** and development validation in **2020**. It compares four horizons and three additional processing delays, keeping the 2021 reserve untouched.

At the primary five-session horizon with no extra delay, adding text reduces MSE by **1.753%** relative to price/volume alone. Against the stronger price/volume-plus-title-count control, MSE **increases by 0.713%**. Information arrival is therefore a necessary control when attributing predictive value to language in this pilot.

![Stage 1: all twelve horizon-by-delay comparisons of headline text against the title-count control. Positive values indicate lower validation MSE.](assets/figures/stage1-content-ablation.png)

*Positive values mean lower error after adding content. The primary comparison has 461 training and 238 validation observations. This is a single-stock, single-year development experiment; the modest 20-session improvements are descriptive.*

### Stage 2.4 · Long-horizon rankings depend on overlap

The disclosure study trains on **2018–2025** and evaluates the available **2026 development window**, with market data through **September 30, 2026**. The full control combines market state, event metadata, and TF-IDF.

![Stage 2.4: full-control pooled and non-overlapping Rank IC at 5, 20, and 60 sessions, with observed sample sizes.](assets/figures/stage24-overlap-sensitivity.png)

| Horizon | Training events | Observed validation outcomes | Pooled Rank IC | Non-overlap outcomes | Non-overlap Rank IC |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 sessions | 276 | 25 | +0.0004 | 22 | +0.0119 |
| 20 sessions | 276 | 25 | −0.2578 | 18 | −0.3849 |
| 60 sessions | 271 | 17 | +0.2454 | 7 | −0.1786 |

The 60-session association changes sign after overlap removal. With only seven remaining outcomes, this finding motivates a larger event panel and stronger dependence controls. The current evidence does **not establish stable incremental alpha**. The 60-session confidence interval is unavailable under the registered minimum of 20 observed outcomes.

<details>
<summary><strong>View all seven registered baselines</strong></summary>

![All registered non-LLM models and horizons. The constant historical-mean baseline has undefined Rank IC.](assets/figures/stage24-model-comparison.png)

“Market state” maps to the implementation's `financial_only` model: available features chiefly describe prices, volume, and market conditions. Nine industry, valuation, and historical accounting columns are missing in this run. The constant historical-mean predictor has undefined Rank IC.

Across the seven models and three horizons, 525 validation predictions come from **25 underlying events**. Adding TF-IDF to market state and metadata changes ranking metrics but produces the same simulated trades under the frozen entry rule. Full numerical values and model identifiers are in the [public snapshot](docs/research/snapshot.json).

</details>

The two stages use different cohorts, time periods, and target conventions. Their results address complementary questions and are reported separately. See the [evidence and methods note](docs/research/README.md) for the precise comparison scope.

## Next: surprise × market state

The next hypothesis is that predictive value concentrates in events with **new fundamental information**, **a verifiable expectation gap**, and **limited prior price response**. A prepared semantic schema separates fundamental direction, earnings quality, surprise, demand, margins, persistence, novelty, and risk, with source-linked evidence for each supported field.

An example registered interaction is:

```math
g_e=S_e\,\Pi_e\left[1-\widehat F_{\mathrm{train}}\!\left(r_e^{\mathrm{prior}}\right)\right],
```

where $S_e$ is evidence-supported earnings or guidance surprise, $\Pi_e$ is persistence, and the empirical CDF turns prior relative return into a priced-in proxy. The default design uses prior industry-relative return; completing its historical benchmark is part of the next data milestone. The proxy measures prior price response, with market expectations remaining a separate object to validate.

The planned **Surprise × Priced-in** grid will report sample counts and realized outcomes in every cell. It is currently unestimated: the audited numerical expectation store and historical industry coverage must be completed before semantic inference. The prepared context bundles one current document with up to three strictly earlier documents; unsupported surprise values remain `null`.

**Why 2026?** It is the development window for examining whether event responses differ in a more algorithmic market. A claim about quantitative crowding will require a separate participation or crowding measure. Future prospective validation will use a newly frozen protocol and later observations.

## Reproduce the figures

The public research package contains aggregate observations, source fingerprints, and a deterministic plotting script. It regenerates the figures above from the frozen snapshot:

```bash
git clone https://github.com/TanhJK728/A-Share-LLM-Trading-Research.git
cd A-Share-LLM-Trading-Research
python -m venv .venv
source .venv/bin/activate
python -m pip install -r docs/research/requirements.txt
python scripts/render_research_figures.py
```

| Artifact | Purpose |
| :--- | :--- |
| [Research snapshot](docs/research/snapshot.json) | All plotted aggregates, model identifiers, sample sizes, and source SHA-256 fingerprints |
| [Methods & evidence](docs/research/README.md) | Cohort definitions, timing, interpretation, and provenance |
| [Figure generator](scripts/render_research_figures.py) | Matplotlib source for PNG and SVG outputs |
| [Vector illustrations](assets/) | Original research overview and pipeline diagrams |

The local Stage 2.4 run passed 373 offline tests, 2,289 independent prediction/threshold/metric checks, and reconciliation of 63 finite-cash portfolio scenarios. These are snapshot-level engineering checks. The public package reproduces the aggregate figures; end-to-end reruns additionally require the local research modules, source documents, and market snapshots.

## Repository context

The original system provides the engineering foundation: AkShare ingestion, ClickHouse/MongoDB storage, Qlib feature export, LightGBM modeling, and experimental sentiment and execution modules.

| Existing code | Role |
| :--- | :--- |
| [`data_ingestion/`](data_ingestion/) | Market, benchmark, concept, and news collection |
| [`data_processing/`](data_processing/) | Qlib-oriented data export |
| [`research/`](research/) | Original factor, sector, and sentiment experiments |
| [`backtest/`](backtest/) · [`trade/`](trade/) | Original simulation and execution prototypes |

The figures on this page document the newer controlled NLP studies. Earlier strategy demonstrations remain in the repository's [history](https://github.com/TanhJK728/A-Share-LLM-Trading-Research/commits/main/README.md). The next research deliverables are historical industry coverage, audited guidance-to-actual links, semantic ablations, and a prospective evaluation with a frozen information set.
