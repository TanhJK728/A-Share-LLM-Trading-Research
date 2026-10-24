#!/usr/bin/env python3
"""Render public research figures from a small, explicitly allowlisted snapshot.

Reproduce the committed figures:
    python -m pip install -r docs/research/requirements.txt
    python scripts/render_research_figures.py

Refresh aggregates from an existing research archive (reads saved results only):
    python scripts/render_research_figures.py --refresh-from-research /path/to/nlp_research

The refresh command performs no fitting, inference, market queries or research
runs. Original source paths are stored relative to the research repository;
absolute paths, source bodies, provider payloads and credentials are excluded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "docs/research/snapshot.json"
OUTPUT = ROOT / "assets/figures"
RUN = "stage2_4_results/run_20261004T235930Z"
MODELS = (
    "historical_mean", "financial_only", "metadata", "tfidf",
    "financial_tfidf", "financial_metadata", "financial_metadata_tfidf",
)
LABELS = {
    "historical_mean": "Historical mean",
    "financial_only": "Market features",
    "metadata": "Event metadata",
    "tfidf": "TF–IDF text",
    "financial_tfidf": "Market + text",
    "financial_metadata": "Market + metadata",
    "financial_metadata_tfidf": "Market + metadata + text",
}
INK = "#142C38"
MUTED = "#65747C"
TEAL = "#137E83"
COPPER = "#B76643"
PAPER = "#FCFCFA"
RULE = "#D9E2E3"
NA = "#E9EEED"


def source_record(path: Path, research: Path, selection: str) -> dict:
    return {
        "original_repository_relative_path": "nlp_research/" + path.relative_to(research).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "selection": selection,
    }


def extract_snapshot(research: Path) -> dict:
    """Copy only the aggregate fields needed to explain and render the figures."""
    research = research.resolve()
    progress = research / "RESEARCH_PROGRESS.md"
    markdown = progress.read_text(encoding="utf-8")
    rows = []
    # The report publishes all 4 horizons x 3 additional-delay conditions to 3 d.p.
    for line in markdown.splitlines():
        fields = [v.strip().replace("**", "") for v in line.split("|")[1:-1]]
        if len(fields) != 5 or not re.fullmatch(r"(?:1|5|20|60) 日(?:（主）)?", fields[0]):
            continue
        train, valid = [int(v) for v in fields[1].split("／")]
        values = [float(v.replace("%", "").replace("−", "-")) for v in fields[2:]]
        rows.append({"horizon_sessions": int(fields[0].split()[0]), "n_train": train,
                     "n_validation": valid, "additional_delay_sessions": [0, 1, 2],
                     "mse_reduction_vs_count_control_pct": values})
    if [r["horizon_sessions"] for r in rows] != [1, 5, 20, 60]:
        raise ValueError("Expected the complete registered Stage 1 horizon/delay table")
    # Preserve the one explicitly reported comparison with the price-only control.
    example = re.search(r"额外延迟 0 时，内容模型相对纯价量的 MSE 降低 ([\d.]+)%，但相对加入标题数量的对照反而增加 ([\d.]+)%", markdown)
    if not example:
        raise ValueError("Stage 1 price/count comparator example changed; review its source")
    metrics_path = research / RUN / "baseline/metrics.json"
    protocol_path = research / RUN / "frozen_protocol.json"
    verification_path = research / RUN / "baseline/verification.json"
    portfolio_verification_path = research / RUN / "portfolio_independent_verification.json"
    tests_path = research / RUN / "test_verification.json"
    metrics = json.loads(metrics_path.read_text())
    protocol = json.loads(protocol_path.read_text())
    results = []
    for horizon in (5, 20, 60):
        for model in MODELS:
            matches = [r for r in metrics["results"] if r["target"] == "csi300" and r["cohort"] == "core"
                       and r["horizon"] == horizon and r["model"] == model]
            if len(matches) != 1 or matches[0]["status"] != "development_ranking_fit_completed":
                raise ValueError("Missing or ambiguous registered model/horizon result")
            row = matches[0]
            nonoverlap = row["nonoverlap_validation"]
            results.append({
                "model": model, "display_label": LABELS[model], "horizon_sessions": horizon,
                "n_train": row["n_train"], "n_validation_predictions": row["n_validation_predictions"],
                "n_observed_outcomes": row["n_observed_validation_outcomes"],
                "n_nonoverlap_observed_outcomes": nonoverlap["n"],
                "n_nonoverlap_pretrade_events": row["overlap"]["greedy_nonoverlap_events"],
                "rank_ic": row["metrics"]["rank_ic"], "pearson_ic": row["metrics"]["pearson_ic"],
                "nonoverlap_rank_ic": nonoverlap["rank_ic"],
                "rank_ic_stock_cluster_ci95": row["uncertainty"]["stock"]["rank_ic_ci95"],
                "rank_ic_month_cluster_ci95": row["uncertainty"]["month"]["rank_ic_ci95"],
            })
    # Every fitted model uses the same event cohort within its horizon.
    for horizon in (5, 20, 60):
        counts = {(r["n_train"], r["n_validation_predictions"], r["n_observed_outcomes"],
                   r["n_nonoverlap_observed_outcomes"]) for r in results if r["horizon_sessions"] == horizon}
        if len(counts) != 1:
            raise ValueError("Model comparison lacks a common cohort")
    verification = json.loads(verification_path.read_text())
    ledger = json.loads(portfolio_verification_path.read_text())
    tests = json.loads(tests_path.read_text())
    sources = {
        "stage1_report": source_record(progress, research, "Published 4x3 ablation table and one 5D comparator example; rounded report values"),
        "stage24_metrics": source_record(metrics_path, research, "Core provisional CSI300 branch; all seven completed models and all three registered horizons"),
        "stage24_protocol": source_record(protocol_path, research, "Train/validation boundaries, frozen target universes, label convention, paid-call count"),
        "point_verification": source_record(verification_path, research, "Status and checked point-metric count only"),
        "portfolio_verification": source_record(portfolio_verification_path, research, "Status and checked-account count only"),
        "test_verification": source_record(tests_path, research, "Status and test count only; execution command excluded"),
    }
    return {
        "schema_version": "research-figures-v1",
        "result_snapshot_date": "2026-10-04",
        "description": "Public aggregate extract from saved research results; contains no raw disclosure bodies or private filesystem references.",
        "provenance": sources,
        "stage1": {
            "study_id": "pilot_development_v1", "security": {"ticker": "000001", "name": "Ping An Bank"},
            "training_years": [2018, 2019], "validation_year": 2020,
            "text_representation": "Training-fitted character-bigram TF-IDF; mixed-source headlines",
            "metric": "100 * (1 - MSE_content_model / MSE_control_model)",
            "primary_control": "Price-volume features + title count and presence",
            "reported_decimal_places": 3, "horizon_unit": "trading sessions",
            "timing_note": "Additional delay 0 already waits until the next session after the source date; Stage 1 uses open-to-open labels.",
            "cohort_note": "Single-stock 2020 development study, conditional on observed holding paths.",
            "rows": rows,
            "price_control_example": {"horizon_sessions": 5, "additional_delay_sessions": 0,
                "mse_reduction_vs_price_only_pct": float(example.group(1)),
                "mse_reduction_vs_count_control_pct": -float(example.group(2)),
                "coverage": "Only this price-only comparison is explicitly reported; no missing conditions inferred."},
        },
        "stage24": {
            "study_id": "run_20261004T235930Z", "target": "CSI300-adjusted event price return",
            "target_role": "provisional_robustness_branch", "cohort": "core",
            "stocks": protocol["universe_by_target"]["csi300"],
            "training_start": protocol["temporal_split"]["train_start"],
            "validation_start": protocol["temporal_split"]["validation_start"],
            "cutoff_exclusive": protocol["data_cutoff_exclusive"], "last_market_session": "2026-09-30",
            "label": "Entry open at i to close at i + H - 1; H includes the entry session.",
            "rank_ic_definition": "Spearman correlation pooled across stock-event observations; average ranks for ties.",
            "nonoverlap_rule": "First chronological event per stock; next entry strictly after its planned exit; keep fitted model and training thresholds unchanged.",
            "model_display_note": "Market features maps to the registered financial_* family; nine industry, valuation and accounting columns are entirely missing in training.",
            "uncertainty_note": "Separate stock/month cluster intervals are descriptive. 60D has 17 outcomes and no interval under the registered minimum of 20.",
            "models": list(MODELS), "results": results,
            "status": {"formal_industry_stock_count": len(protocol["universe_by_target"]["industry"]),
                "paid_llm_requests": protocol["paid_inference_calls"],
                "semantic_and_surprise_models": "not_run", "alpha_status": "not_established"},
        },
        "verification": {"saved_point_metric_status": verification["status"],
            "saved_point_metric_count": verification["checked_point_metrics"],
            "saved_ledger_status": ledger["status"], "saved_ledger_accounts": ledger["checked_accounts"],
            "saved_offline_test_status": tests["status"], "saved_offline_test_count": tests["test_count"],
            "scope": "Saved engineering verification counts; no new research or tests run by this renderer."},
    }


def configure_plotting():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11, "text.color": INK,
        "axes.labelcolor": INK, "xtick.color": MUTED, "ytick.color": INK,
        "axes.facecolor": PAPER, "figure.facecolor": PAPER, "savefig.facecolor": PAPER,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.spines.left": False, "axes.spines.bottom": False,
        "svg.fonttype": "path", "svg.hashsalt": "a-share-research-v1",
    })
    cmap = LinearSegmentedColormap.from_list("copper_paper_teal", [COPPER, "#F5F6F2", TEAL], N=256)
    cmap.set_bad(NA)
    return plt, cmap


def heading(fig, label, title, subtitle):
    fig.text(.055, .947, label, fontsize=10, weight="bold", color=TEAL)
    fig.text(.055, .884, title, fontsize=23, weight="bold", color=INK)
    fig.text(.055, .837, subtitle, fontsize=10.4, color=MUTED)


def source_footer(fig, snapshot, source_id, description):
    source = snapshot["provenance"][source_id]
    fig.text(.055, .058, description, fontsize=9.3, color=MUTED)
    fig.text(.055, .029, "Source: " + Path(source["original_repository_relative_path"]).name
             + "  ·  SHA256 " + source["sha256"][:12] + "…  ·  Full provenance: docs/research/snapshot.json",
             fontsize=8, color=MUTED)


def save_figure(fig, stem):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT / (stem + ".png"), dpi=180, metadata={"Software": "Matplotlib; scripts/render_research_figures.py"})
    fig.savefig(OUTPUT / (stem + ".svg"), metadata={"Date": None, "Creator": "scripts/render_research_figures.py"})
    svg_path = OUTPUT / (stem + ".svg")
    svg_path.write_text("\n".join(line.rstrip() for line in svg_path.read_text().splitlines()) + "\n")


def stage1_figure(plt, cmap, snapshot):
    import numpy as np
    fig = plt.figure(figsize=(12.8, 7.3))
    heading(fig, "01 / HEADLINE CONTENT", "Does content add predictive information?",
            "Stage 1  ·  Ping An Bank  ·  Train 2018–2019  ·  Validation 2020  ·  All registered horizons and delays")
    ax = fig.add_axes([.16, .275, .41, .455])
    rows = snapshot["stage1"]["rows"]
    values = np.array([row["mse_reduction_vs_count_control_pct"] for row in rows])
    im = ax.imshow(values, cmap=cmap, vmin=-1.5, vmax=1.5, aspect="auto")
    ax.set_xticks(range(3), ["+0 sessions", "+1 session", "+2 sessions"])
    ax.xaxis.tick_top()
    ax.tick_params(axis="both", length=0, pad=12)
    ax.set_yticks(range(4), ["1 session", "5 sessions*", "20 sessions", "60 sessions"])
    for i in range(4):
        for j in range(3):
            value = values[i, j]
            ax.text(j, i, f"{value:+.3f}%", ha="center", va="center", fontsize=14,
                    weight="bold", color="white" if abs(value) >= .95 else INK)
    ax.set_xticks(np.arange(-.5, 3, 1), minor=True)
    ax.set_yticks(np.arange(-.5, 4, 1), minor=True)
    ax.grid(which="minor", color=PAPER, linewidth=4)
    ax.tick_params(which="minor", length=0)
    fig.text(.16, .775, "MSE reduction vs price + title-count control", fontsize=11.2, weight="bold")
    fig.text(.16, .235, "* Primary horizon  ·  positive values indicate lower error", fontsize=9.5, color=MUTED)
    cax = fig.add_axes([.16, .175, .41, .018])
    cb = fig.colorbar(im, cax=cax, orientation="horizontal", ticks=[-1.5, 0, 1.5])
    cb.outline.set_visible(False)
    cb.ax.set_xticklabels(["−1.5%", "0", "+1.5%"])
    cb.ax.tick_params(length=0, labelsize=9, pad=5)
    right = fig.add_axes([.68, .37, .25, .29])
    example = snapshot["stage1"]["price_control_example"]
    numbers = [example["mse_reduction_vs_price_only_pct"], example["mse_reduction_vs_count_control_pct"]]
    right.barh([1, 0], numbers, height=.29, color=[TEAL, COPPER])
    right.axvline(0, color=MUTED, linewidth=.8)
    right.set_xlim(-1.1, 2.2)
    right.set_ylim(-.6, 1.65)
    right.set_xticks([-1, 0, 1, 2], ["−1%", "0", "+1%", "+2%"])
    right.tick_params(axis="both", length=0)
    right.set_yticks([])
    for y, label, value, color in zip([1, 0], ["vs price only", "vs price + count"], numbers, [TEAL, COPPER]):
        right.text(-1.05, y+.28, label, fontsize=10.3, color=INK)
        right.text(value+(.07 if value>0 else -.07), y, f"{value:+.3f}%",
                   va="center", ha="left" if value>0 else "right", fontsize=11, weight="bold", color=color)
    fig.text(.68, .775, "The comparator matters", fontsize=14, weight="bold")
    fig.text(.68, .733, "Same content model · 5 sessions · delay 0", fontsize=9.4, color=MUTED)
    fig.text(.68, .265, "Title count captures part of the gain.\nAt the primary horizon, text adds\nno improvement over that control.",
             fontsize=11, linespacing=1.6, color=INK)
    source_footer(fig, snapshot, "stage1_report",
        "Single-stock development cohort. Reported values are rounded to 3 decimals; delay 0 already waits until the next session.")
    save_figure(fig, "stage1-content-ablation")
    plt.close(fig)


def model_figure(plt, cmap, snapshot):
    import numpy as np
    fig = plt.figure(figsize=(12.8, 8.1))
    heading(fig, "02 / DISCLOSURE-TEXT BASELINES", "Text signal changes with the horizon",
        "Stage 2.4  ·  Five-stock provisional CSI300 branch  ·  Train 2018–2025  ·  2026 development validation")
    ax = fig.add_axes([.31, .238, .48, .51])
    data = snapshot["stage24"]["results"]
    lookup = {(r["model"], r["horizon_sessions"]): r for r in data}
    values = np.array([[lookup[(m, h)]["rank_ic"] if lookup[(m, h)]["rank_ic"] is not None else np.nan
                       for h in (5, 20, 60)] for m in MODELS])
    im = ax.imshow(np.ma.masked_invalid(values), cmap=cmap, vmin=-.6, vmax=.6, aspect="auto")
    ax.set_xticks(range(3), ["5 sessions\n25 outcomes", "20 sessions\n25 outcomes", "60 sessions\n17 outcomes"])
    ax.xaxis.tick_top()
    ax.set_yticks(range(7), [LABELS[m] for m in MODELS])
    ax.tick_params(axis="both", length=0, pad=12, labelsize=11.2)
    ax.get_yticklabels()[-1].set_weight("bold")
    ax.get_yticklabels()[3].set_weight("bold")
    for i in range(7):
        for j in range(3):
            value = values[i, j]
            ax.text(j, i, "Undefined" if np.isnan(value) else f"{value:+.4f}", ha="center", va="center",
                    fontsize=10 if np.isnan(value) else 14, weight="normal" if np.isnan(value) else "bold",
                    color=MUTED if np.isnan(value) else ("white" if abs(value)>.33 else INK))
    ax.set_xticks(np.arange(-.5, 3, 1), minor=True)
    ax.set_yticks(np.arange(-.5, 7, 1), minor=True)
    ax.grid(which="minor", color=PAPER, linewidth=4)
    ax.tick_params(which="minor", length=0)
    cax = fig.add_axes([.83, .238, .018, .51])
    cb = fig.colorbar(im, cax=cax, ticks=[-.6, -.3, 0, .3, .6])
    cb.outline.set_visible(False)
    cb.ax.tick_params(length=0, pad=6, labelsize=10)
    cb.set_label("Pooled event Rank IC (Spearman)", labelpad=17, fontsize=10.5)
    fig.text(.31, .178, "Shared event cohort within each horizon · fixed Ridge penalty · training-fitted TF–IDF", fontsize=10, color=INK)
    fig.text(.055, .122, "Market features = the registered financial_* family; nine industry, valuation and accounting columns are all missing.\nHistorical-mean scores are constant, so their correlation is undefined. All seven fitted models are shown.",
             fontsize=9.3, color=MUTED, linespacing=1.6)
    source_footer(fig, snapshot, "stage24_metrics",
        "25 distinct validation events. 60-session labels are mature for 17; formal industry and LLM models remain unestimated.")
    save_figure(fig, "stage24-model-comparison")
    plt.close(fig)


def overlap_figure(plt, cmap, snapshot):
    fig = plt.figure(figsize=(12.8, 7.3))
    heading(fig, "03 / OVERLAP SENSITIVITY", "The long-horizon result depends on overlap",
        "Stage 2.4  ·  Market + metadata + TF–IDF  ·  Same fitted model and training thresholds  ·  CSI300-adjusted target")
    rows = [r for r in snapshot["stage24"]["results"] if r["model"] == "financial_metadata_tfidf"]
    ax = fig.add_axes([.16, .27, .52, .43])
    ax.axvline(0, color=RULE, linewidth=1.5, zorder=0)
    for i, row in enumerate(rows):
        y = 2-i
        a, b = row["rank_ic"], row["nonoverlap_rank_ic"]
        ax.plot([a, b], [y, y], color="#A8B9BA", linewidth=3, zorder=1)
        ax.scatter(a, y, s=115, color=TEAL, edgecolors=PAPER, linewidths=1.3, zorder=3)
        ax.scatter(b, y, s=105, color=COPPER, marker="D", edgecolors=PAPER, linewidths=1.3, zorder=4)
        ax.annotate(f"{a:+.4f}", (a, y), xytext=(0, 15), textcoords="offset points", ha="center",
                    fontsize=11, color=TEAL, weight="bold")
        ax.annotate(f"{b:+.4f}", (b, y), xytext=(0, -22), textcoords="offset points", ha="center",
                    fontsize=11, color=COPPER, weight="bold")
    ax.set_yticks([2, 1, 0], ["5 sessions", "20 sessions", "60 sessions"])
    ax.set_xlim(-.5, .4)
    ax.set_ylim(-.5, 2.5)
    ax.set_xticks([-.4, -.2, 0, .2, .4], ["−0.4", "−0.2", "0", "+0.2", "+0.4"])
    ax.tick_params(length=0, pad=12)
    ax.set_xlabel("Pooled event Rank IC", labelpad=13)
    from matplotlib.lines import Line2D
    legend = [Line2D([0], [0], color=TEAL, marker="o", markersize=7, linestyle="None", label="All observed events"),
              Line2D([0], [0], color=COPPER, marker="D", markersize=7, linestyle="None", label="Greedy nonoverlap subset")]
    fig.legend(handles=legend, loc="upper left", bbox_to_anchor=(.15, .785), ncol=2, frameon=False,
               handletextpad=.45, columnspacing=2.0, fontsize=10.2)
    panel = fig.add_axes([.74, .27, .21, .43])
    panel.set_axis_off()
    panel.text(0, 1.12, "OBSERVED SAMPLES", fontsize=9.5, color=MUTED, weight="bold")
    for i, row in enumerate(rows):
        y = (2-i+.5)/3
        panel.text(0, y, str(row["n_observed_outcomes"]), fontsize=20, weight="bold", color=TEAL, va="center")
        panel.text(.26, y, "→", fontsize=17, color=MUTED, va="center")
        panel.text(.48, y, str(row["n_nonoverlap_observed_outcomes"]), fontsize=20, weight="bold", color=COPPER, va="center")
    fig.text(.16, .13, "At 60 sessions, Rank IC moves from +0.2454 to −0.1786 with only seven nonoverlapping outcomes.\nThe registered minimum for uncertainty intervals is 20 observed events; 60-session intervals remain unavailable.",
             fontsize=10.3, color=INK, linespacing=1.6)
    source_footer(fig, snapshot, "stage24_metrics",
        "Greedy selection uses entry dates and planned exits. Five immature nonoverlap events remain in signals and outside observed metrics.")
    save_figure(fig, "stage24-overlap-sensitivity")
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-from-research", type=Path, help="Read existing aggregate reports and refresh the public snapshot")
    args = parser.parse_args(argv)
    if args.refresh_from_research:
        snapshot = extract_snapshot(args.refresh_from_research)
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    plt, cmap = configure_plotting()
    stage1_figure(plt, cmap, snapshot)
    model_figure(plt, cmap, snapshot)
    overlap_figure(plt, cmap, snapshot)
    print("Rendered 3 research figures in PNG and SVG from " + SNAPSHOT.relative_to(ROOT).as_posix())


if __name__ == "__main__":
    main()
