# Research snapshot: methods and provenance

This package publishes aggregate evidence from two local research stages and the code needed to redraw the figures. Their original project-relative source paths and SHA-256 fingerprints accompany the values in [snapshot.json](snapshot.json).

## Stage 1: headline-content pilot

The cohort is Ping An Bank. Training uses 2018–2019; development validation uses 2020. The 2021 reserve was not evaluated. The pilot uses 4,208 mixed-source headline rows, including 33 repeated headlines, across 730 trading dates. Media, brokerage, and social sources are mixed, so the estimates do not identify an individual source's contribution.

Three fixed feature sets are compared: price/volume; price/volume with title count and presence; and the same features with character-bigram TF-IDF. Four horizons and three additional processing delays produce 36 model/horizon/delay conditions. The heatmap contains all twelve content-versus-count comparisons:

```math
\Delta_{H,\ell}=100\left(1-\frac{\mathrm{MSE}_{\mathrm{text},H,\ell}}{\mathrm{MSE}_{\mathrm{count},H,\ell}}\right).
```

Positive values indicate lower validation error. Zero extra delay already applies the next-session availability assumption for date-only titles. This pilot uses open-to-open holding labels, separately from the Stage 2.4 convention. It requires an observable active holding path, so estimates are conditional prediction results.

Values are transcribed at the precision recorded in `nlp_research/RESEARCH_PROGRESS.md`. The +1.753% price-only comparison is reported for the primary five-session, zero-extra-delay example; the heatmap values compare text against the title-count control.

## Stage 2.4: disclosure-event study

**Frozen run:** `run_20261004T235930Z`

**Freeze manifest SHA-256:** `52bb8e436fd66748a888ea077dfb4dd301ca5e09d897b8fac62cbd8d29f1fb46`

**Data cutoff:** October 4, 2026, exclusive; latest market session September 30.

**Training:** 2018–2025, with cross-boundary labels removed.

**Validation:** available 2026 development period; this window has been inspected during research.

### Cohort and denominators

The original 20 candidates yield nine stocks listed before 2018. Five meet the frozen minimum of 20 training and three mature validation events at every primary horizon for the provisional CSI 300 branch: `300476`, `300308`, `300502`, `002384`, and `002156`. Historical industry coverage is insufficient for the formal industry-adjusted branch.

There are 306 unique events. Three horizons generate 918 candidate event-horizon pairs; five training 60-session labels crossing into 2026 are excluded, leaving 913 rows. Of these, 890 have observed targets, 15 lack a valid complete price path, and eight are immature.

Each of seven models predicts 25 validation events at three horizons, yielding 525 predictions. Observed outcomes number 25, 25, and 17. Predictions are retained when a future outcome is not yet observable, separating prediction availability from label availability.

### Representations and controls

The lexical baseline uses character bigrams, at most 512 vocabulary items, training-only document frequencies, sublinear term frequency, smoothed IDF, and row L2 normalization. Ridge regularization is fixed at 10 for the sum-of-squares objective; the intercept is unpenalized.

Code identifiers are `historical_mean`, `financial_only`, `metadata`, `tfidf`, `financial_tfidf`, `financial_metadata`, and `financial_metadata_tfidf`. Figures label the available financial family **Market state**, because nine industry, valuation, and historical accounting columns are entirely absent from training and retain missing values and indicators. Original model identifiers remain in the snapshot.

Vocabulary, IDF, numeric imputation/standardization, score buckets, and the 80th-percentile entry threshold use training data only. Model selection and hyperparameter search were not performed on the reported 2026 rankings.

### Timing and target conventions

For a date-only disclosure, pre-event state ends at 00:00 on its recorded disclosure date. Candidate entry is the first supplied calendar open strictly after the end-of-date availability bound plus five minutes. Exact timestamps similarly use the first open after timestamp and processing allowance. Five minutes is a simulation assumption, not measured LLM latency.

The label runs from stock entry open at session $i$ through close at $i+H-1$, less the corresponding CSI 300 return. Complete positive and valid active paths are required for observed labels; missing paths receive no fill or shifted endpoint. The previous Stage 2 convention used $i+H$ and remains a separate historical experiment.

Original body vintage and first public availability are not fully established for all historical sources. Provenance records and conservative timing rules make those source assumptions explicit.

### Evaluation and uncertainty

Rank IC is Spearman correlation pooled over stock-events with observed outcomes. The constant historical-mean predictor has undefined correlation, stored as `null`. The overlap sensitivity greedily retains non-overlapping holding windows within each stock, leaving 22, 18, and seven observed events at 5, 20, and 60 sessions. Common market exposure and other dependence may remain.

The local metrics include separate stock-cluster and month-cluster bootstrap diagnostics with 400 replicates. Intervals are descriptive, conditional on fitted models, and unadjusted for multiple comparisons; month clusters do not remove cross-month holding overlap. The 60-session interval is unavailable because only 17 observed events remain, below the registered minimum of 20. Figures show point estimates and counts, without significance markers.

Portfolio simulations use five cash-constrained slots, one position per stock, separate horizon accounts, and 10/30/50 basis-point round-trip costs. All 63 scenarios reconciled under daily-bar assumptions. These simulations assume divisible units and do not validate auction fills or corporate-action cash flows. The README focuses on predictive comparisons; the original repository's strategy-return illustration is a separate experiment.

### Semantic extension

The prepared schema contains 15 fields: fundamental direction, earnings quality, earnings surprise, guidance surprise, demand change, margin change, capacity change, customer/product change, persistence, novelty, management confidence, risk change, structural change, evidence strength, and confidence.

Each non-null output requires a source reference and a locatable quotation. Context contains one current document and up to three strictly earlier documents. Quote matching checks source presence; semantic entailment remains an evaluation task. Historical use of a contemporary language model also requires assessing knowledge contamination.

Numerical surprise requires an audited expectation baseline and consistent accounting units and periods. This run has no completed numerical training expectation store and zero LLM inference requests. Surprise interactions and all four Surprise × Priced-in cells remain unestimated. The interaction code defaults to the pre-event 20-session industry-relative return's training ECDF as its priced-in proxy; current CSI 300 results do not estimate this interaction.

## Reproduction and release scope

Run the [figure generator](../../scripts/render_research_figures.py) with the [pinned dependencies](requirements.txt). It reads [snapshot.json](snapshot.json) and writes PNG/SVG files under `assets/figures/`; no data download, model fitting, or LLM call is involved. The two overview illustrations are original static SVGs.

Fingerprints identify source artifacts for comparison when originals are available. The aggregate release reproduces figures; original fits additionally require the local research modules and source data. Local verification recorded 373 passing offline tests and 2,289 prediction, threshold, and metric checks. Raw document bodies, account configuration, and local absolute paths are excluded from this release.
