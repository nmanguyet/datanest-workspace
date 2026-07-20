import gc
import glob
import json
import os
import random
import time
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import shap
from catboost import CatBoostClassifier, Pool
from scipy.stats import wilcoxon, ttest_rel
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

# ==============================================================================
# CONFIGURATION
# ==============================================================================
@dataclass
class NewFeatureEvalConfig:
    # Data paths
    base_features_path: str = "/user/nguyetnguyen/tmp/test_new_features/base_df/hc_2025_all_features"
    new_features_path: str = "/user/nguyetnguyen/features/hc_2025/device_imei_merge_all"
    output_dir: str = "./experiment/device"
    all_features_csv: str = "./all_features.csv"

    # Split
    split_date: str = "2024-07-01"

    # Sampling
    base_feature_sample_size: int = 100
    n_experiments: int = 20
    seed: int = 42
    sample_ratio: Optional[float] = None
    max_rows_per_experiment: Optional[int] = None
    data_filter: Optional[str] = None

    # Row sampling strategy per experiment: "fixed" | "random" | "filter"
    row_sample_strategy: str = "fixed"
    row_filters: Optional[list] = None

    # Column names
    target_col: str = "label_value"
    join_keys: list = field(default_factory=lambda: ["phone_number", "disburse_date"])
    ignore_columns: list = field(default_factory=lambda: ["phone_number", "disburse_date", "product", "label_value"])

    # Features to remove before sampling
    removed_features: list = field(default_factory=list)

    # Pattern-based column type casting
    categorical_to_numeric_patterns_ggsn: list = field(default_factory=list)
    categorical_to_numeric_patterns_sms: list = field(default_factory=list)
    numeric_to_categorical_patterns_others: list = field(default_factory=list)

    # Baseline random noise
    baseline_random: bool = False
    random_col: str = "__noise__"

    # SHAP (only computed after evaluation for accepted / top-N features)
    top_n_for_shap: int = 0
    shap_n_samples: int = 1000

    # CatBoost parameters
    catboost_params: dict = field(default_factory=lambda: {
        "bagging_temperature": 0.1,
        "border_count": 32,
        "random_seed": 42,
        "max_depth": 4,
        "l2_leaf_reg": 12,
        "iterations": 5000,
        "leaf_estimation_iterations": 5,
        "learning_rate": 0.025,
        "custom_metric": ["AUC:hints=skip_train~false"],
        "thread_count": 15,
        "verbose": 100,
        "auto_class_weights": "Balanced",
    })

    # Scoring thresholds
    reject_gates: dict = field(default_factory=lambda: {
        "stability": 0.15,
        "win_rate": 0.50,
    })
    stability: dict = field(default_factory=lambda: {
        "thresholds": [3, 2, 1, 0.5, 0],
        "scores": [35, 30, 24, 16, 8],
    })
    win_rate: dict = field(default_factory=lambda: {
        "thresholds": [0.9, 0.8, 0.7, 0.6, 0.55, 0.50, 0],
        "scores": [25, 22, 18, 14, 10, 6, 0],
    })
    dominance: dict = field(default_factory=lambda: {
        "thresholds": [0.9, 0.8, 0.7, 0.6, 0.55, 0.50, 0.45, 0],
        "scores": [20, 17, 14, 10, 7, 4, 2, 0],
    })
    p_value: dict = field(default_factory=lambda: {
        "thresholds": [6, 5, 4, 3, 2, 1],
        "scores": [5, 4, 3, 2, 1, 0],
    })
    ci_lo: dict = field(default_factory=lambda: {
        "thresholds": [0.5, 0.3, 0.1, 0],
        "scores": [20, 15, 8, 0],
    })
    median_gap: dict = field(default_factory=lambda: {
        "thresholds": [0.5, 0.3, 0.1, 0],
        "scores": [10, 7, 4, 0]
    })
    decision_gates: list = field(default_factory=lambda: [35, 55, 75])
    decision_labels: list = field(default_factory=lambda: ["reject", "weak_accept", "accept", "strong_accept"])

    @classmethod
    def from_json(cls, path: str = "./experiment_config.json") -> "NewFeatureEvalConfig":
        with open(path) as f:
            data = json.load(f)
        valid_keys = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid_keys})

    @property
    def split_date_ts(self) -> pd.Timestamp:
        return pd.Timestamp(self.split_date)

    @property
    def cast_config(self) -> dict:
        return {
            "ggsn": self.categorical_to_numeric_patterns_ggsn,
            "sms": self.categorical_to_numeric_patterns_sms,
            "others": self.numeric_to_categorical_patterns_others,
        }

    def asdict(self) -> dict:
        return asdict(self)


# ==============================================================================
# HELPERS
# ==============================================================================
def _random_col_gen(n: int) -> np.ndarray:
    return np.random.uniform(0, 1, n)


def _score_from_thresholds(value: float, thresholds: list, scores: list) -> int:
    return scores[next((i for i, t in enumerate(thresholds) if value >= t), -1)]


def save_experiment_config(
    output_dir: str,
    global_config: dict,
    per_experiment_configs: Optional[list] = None,
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "experiment_config.json")
    with open(path, "w") as f:
        json.dump(
            {"global": global_config, "experiments": per_experiment_configs or []},
            f,
            indent=2,
            default=str,
        )
    print(f"Config saved to {path}")

# ==============================================================================
# DATA LOADING & PREPROCESSING
# ==============================================================================
def load_and_join_in_spark(spark: SparkSession, cfg: NewFeatureEvalConfig) -> "pyspark.sql.DataFrame":
    base_df = spark.read.parquet(cfg.base_features_path)
    new_df = spark.read.parquet(cfg.new_features_path)

    if cfg.data_filter:
        base_df = base_df.filter(cfg.data_filter)

    all_feature_cols = [
        c for c in base_df.columns + new_df.columns
        if c not in cfg.removed_features + cfg.ignore_columns + cfg.join_keys + [cfg.target_col]
    ]

    keep_cols = cfg.join_keys + [cfg.target_col] + all_feature_cols
    joined = base_df.select(*[c for c in keep_cols if c in base_df.columns]).join(
        new_df.select(*[c for c in keep_cols if c in new_df.columns and c != cfg.target_col]),
        on=cfg.join_keys,
        how="left",
    )

    cast_patterns = set(sum(cfg.cast_config.values(), []))
    for f in joined.schema:
        dtype = f.dataType.simpleString()
        if any(p in f.name for p in cast_patterns) or dtype.startswith("decimal"):
            joined = joined.withColumn(f.name, F.col(f.name).cast("double"))

    joined.persist(StorageLevel.MEMORY_AND_DISK)
    return joined


def select_experiment_data(
    spark_df,
    base_feature_cols: list,
    new_feature_cols: list,
    cfg: NewFeatureEvalConfig,
    experiment_id: int = 0,
) -> pd.DataFrame:
    all_cols = list(dict.fromkeys(cfg.join_keys + [cfg.target_col] + base_feature_cols + new_feature_cols))
    available = [c for c in all_cols if c in spark_df.columns]
    df = spark_df.select(*available)

    strategy = cfg.row_sample_strategy

    if strategy == "filter":
        filter_expr = None
        if cfg.row_filters and experiment_id < len(cfg.row_filters) and cfg.row_filters[experiment_id]:
            filter_expr = cfg.row_filters[experiment_id]
        if filter_expr:
            df = df.filter(filter_expr)
        if cfg.max_rows_per_experiment is not None:
            n_total = df.count()
            df = df.sample(n=min(cfg.max_rows_per_experiment, n_total), seed=cfg.seed + experiment_id)
        elif cfg.sample_ratio is not None:
            df = df.sample(fraction=cfg.sample_ratio, seed=cfg.seed + experiment_id)

    elif strategy == "random":
        exp_seed = cfg.seed + experiment_id
        if cfg.max_rows_per_experiment is not None:
            n_total = df.count()
            df = df.sample(n=min(cfg.max_rows_per_experiment, n_total), seed=exp_seed)
        elif cfg.sample_ratio is not None:
            df = df.sample(fraction=cfg.sample_ratio, seed=exp_seed)

    else:
        if cfg.sample_ratio is not None:
            df = df.sample(fraction=cfg.sample_ratio, seed=cfg.seed)
        if cfg.max_rows_per_experiment is not None:
            n_total = df.count()
            df = df.sample(n=min(cfg.max_rows_per_experiment, n_total), seed=cfg.seed)

    return df.toPandas()


def cast_columns_to_numeric(df: pd.DataFrame, cast_config: dict) -> pd.DataFrame:
    patterns = set(sum(cast_config.values(), []))
    cols_to_cast = list({c for p in patterns for c in df.columns if p in c})
    if cols_to_cast:
        df[cols_to_cast] = df[cols_to_cast].astype(float)
    return df


def preprocess_dataframe(
    df: pd.DataFrame,
    cfg: NewFeatureEvalConfig,
    categorical_cols: Optional[list] = None,
    date_col: str = "disburse_date",
) -> pd.DataFrame:
    categorical_cols = categorical_cols or []
    df = df.copy()

    if cfg.cast_config:
        df = cast_columns_to_numeric(df, cfg.cast_config)

    df[date_col] = pd.to_datetime(df[date_col])
    feature_cols = [c for c in df.columns if c not in cfg.ignore_columns]
    numeric_cols = [c for c in feature_cols if c not in categorical_cols and c != date_col]

    if numeric_cols:
        df[numeric_cols] = df[numeric_cols].fillna(-1.0)
    if categorical_cols:
        df[categorical_cols] = df[categorical_cols].fillna("")

    df = df.drop_duplicates(subset=cfg.join_keys).sort_values(date_col)
    df["disburse_month"] = df[date_col].dt.to_period("M").astype(str)
    return df


def split_train_test_by_date(
    df: pd.DataFrame,
    date_col: str = "disburse_date",
    split_date: Optional[pd.Timestamp] = None,
) -> tuple:
    split_date = split_date or pd.Timestamp("2024-07-01")
    train_df = df[df[date_col] < split_date]
    test_df = df[df[date_col] >= split_date]
    return train_df, test_df


# ==============================================================================
# FEATURE SAMPLINNG
# ==============================================================================
def load_all_features(cfg: NewFeatureEvalConfig) -> list:
    return pd.read_csv(cfg.all_features_csv)["feature"].tolist()


def generate_feature_samples(
    all_features: list,
    sample_size: int,
    n_experiments: int,
    seed: int = 42,
) -> list:
    rng = random.Random(seed)
    return [rng.sample(all_features, sample_size) for _ in range(n_experiments)]


# ==============================================================================
# MODEL TRAINING & EVALUATION
# ==============================================================================
def train_catboost_model(
    train_df: pd.DataFrame,
    feature_cols: list,
    categorical_cols: list,
    cfg: NewFeatureEvalConfig,
    validation_ratio: float = 0.2,
) -> tuple:
    start_time = time.time()
    model_params = cfg.catboost_params.copy()

    train_split, val_split = train_test_split(
        train_df,
        test_size=validation_ratio,
        random_state=cfg.seed,
        stratify=train_df[cfg.target_col],
    )

    train_pool = Pool(
        train_split[feature_cols],
        train_split[cfg.target_col],
        cat_features=categorical_cols,
    )
    val_pool = Pool(
        val_split[feature_cols],
        val_split[cfg.target_col],
        cat_features=categorical_cols,
    )

    model = CatBoostClassifier(**model_params)
    model.fit(train_pool, eval_set=val_pool, early_stopping_rounds=50, use_best_model=True)
    best_iteration = model.get_best_iteration() + 1

    del model
    gc.collect()

    final_model = CatBoostClassifier(**{**model_params, "iterations": best_iteration})
    
    full_pool = Pool(
        train_df[feature_cols],
        train_df[cfg.target_col],
        cat_features=categorical_cols,
    )
    final_model.fit(full_pool)

    return final_model, best_iteration, time.time() - start_time


def predict_probability(
    model: CatBoostClassifier,
    df: pd.DataFrame,
    feature_cols: list,
    categorical_cols: list,
) -> np.ndarray:
    pool = Pool(df[feature_cols], cat_features=categorical_cols)
    return model.predict_proba(pool)[:, 1]


def compute_shap_values(
    model: CatBoostClassifier,
    df: pd.DataFrame,
    feature_cols: list,
    categorical_cols: list,
    n_samples: int = 1000,
) -> pd.DataFrame:
    sample_df = df[feature_cols].sample(n=min(n_samples, len(df)), random_state=42)
    pool = Pool(sample_df, cat_features=categorical_cols)
    explainer = shap.TreeExplainer(model)
    shap_vals = explainer.shap_values(pool)
    if isinstance(shap_vals, list):
        shap_vals = shap_vals[1]
    return pd.DataFrame(shap_vals, columns=feature_cols)
    
# ==============================================================================
# SINGLE EXPERIMENT RUNNER
# ==============================================================================
def run_single_experiment(
    experiment_id: int,
    base_feature_cols: list,
    new_feature_cols: list,
    spark_df,
    experiment_output_dir: str,
    cfg: NewFeatureEvalConfig,
) -> dict:
    print(f"Running experiment {experiment_id}")

    merged_df = select_experiment_data(spark_df, base_feature_cols, new_feature_cols, cfg, experiment_id)

    categorical_cols = [
        c for c in merged_df.select_dtypes(include=["object", "category"]).columns
        if c not in cfg.ignore_columns
    ]

    base_model_features = base_feature_cols
    new_model_features = base_feature_cols + new_feature_cols
    base_cat_cols = [c for c in base_model_features if c in categorical_cols]
    new_cat_cols = [c for c in new_model_features if c in categorical_cols]

    merged_df = preprocess_dataframe(merged_df, cfg, categorical_cols)
    train_df, test_df = split_train_test_by_date(merged_df, split_date=cfg.split_date_ts)

    if cfg.baseline_random:
        train_df[cfg.random_col] = _random_col_gen(len(train_df))
        test_df[cfg.random_col] = _random_col_gen(len(test_df))
        base_model_features = base_feature_cols + [cfg.random_col]

    del merged_df
    gc.collect()

    baseline_model, _, _ = train_catboost_model(
        train_df, base_model_features, base_cat_cols, cfg,
    )
    baseline_proba = predict_probability(baseline_model, test_df, base_model_features, base_cat_cols)

    new_model, _, _ = train_catboost_model(
        train_df, new_model_features, new_cat_cols, cfg,
    )
    new_proba = predict_probability(new_model, test_df, new_model_features, new_cat_cols)

    del train_df
    gc.collect()

    if cfg.top_n_for_shap > 0:
        model_dir = os.path.join(experiment_output_dir, "models")
        os.makedirs(model_dir, exist_ok=True)
        baseline_model.save_model(os.path.join(model_dir, f"baseline_{experiment_id}.cbm"))
        new_model.save_model(os.path.join(model_dir, f"new_{experiment_id}.cbm"))

        feat_config = {
            "base_model_features": base_model_features,
            "new_model_features": new_model_features,
            "base_cat_cols": base_cat_cols,
            "new_cat_cols": new_cat_cols,
        }
        with open(os.path.join(model_dir, f"feat_config_{experiment_id}.json"), "w") as f:
            json.dump(feat_config, f)

        test_df.to_parquet(
            os.path.join(model_dir, f"test_data_{experiment_id}.parquet"),
            index=False,
        )

    pred_df = pd.DataFrame({
        "y_true": test_df[cfg.target_col].values,
        "baseline_proba": baseline_proba,
        "new_feature_proba": new_proba,
    })
    pred_df.to_parquet(
        os.path.join(experiment_output_dir, f"experiment_{experiment_id}.parquet"),
        index=False,
    )

    del test_df, pred_df, baseline_model, new_model
    gc.collect()

    return {
        "experiment_id": experiment_id,
        "n_base_features": len(base_feature_cols),
        "n_new_features": len(new_feature_cols),
    }


# ==============================================================================
# METRICS & SCORING
# ==============================================================================
def bootstrap_mean_ci(values: np.ndarray, n_resamples: int = 10_000, ci: float = 0.95) -> tuple:
    rng = np.random.default_rng(42)
    samples = rng.choice(values, size=(n_resamples, len(values)), replace=True)
    boot_means = samples.mean(axis=1)
    alpha = (1 - ci) / 2
    return float(np.quantile(boot_means, alpha)), float(np.quantile(boot_means, 1 - alpha))


def calculate_experiment_metrics(results_df: pd.DataFrame) -> dict:
    base_auc = results_df["auc_base"].values
    new_auc = results_df["auc_new"].values
    auc_gap = results_df["gap"].values
    n = len(results_df)
    mean_gap = auc_gap.mean()
    median_gap = auc_gap.median()
    std_gap = auc_gap.std()

    if std_gap == 0:
        _, p_wilcoxon = np.nan, np.nan
        _, p_ttest = np.nan, np.nan
    else:
        _, p_wilcoxon = wilcoxon(new_auc, base_auc, alternative="two-sided")
        _, p_ttest = ttest_rel(new_auc, base_auc)

    n_pairs = len(new_auc) * len(base_auc)
    dominance = np.sum(new_auc[:, None] > base_auc[None, :]) / n_pairs

    ci_lo, ci_hi = bootstrap_mean_ci(auc_gap)

    return {
        "mean_gap": mean_gap,
        "median_gap": median_gap,
        "std_gap": std_gap,
        "ci_lo": ci_lo,
        "ci_hi": ci_hi,
        "win_rate": np.mean(auc_gap > 0),
        "loss_rate": np.mean(auc_gap < 0),
        "dominance": dominance,
        "effect_size": mean_gap / std_gap if std_gap > 0 else (np.inf if mean_gap != 0 else np.nan),
        "p_value_ttest": p_ttest,
        "p_value_wilcoxon": p_wilcoxon,
        "n_experiments": n,
    }


def evaluate_feature(metrics: dict, cfg: NewFeatureEvalConfig) -> dict:
    g = cfg.reject_gates
    stability = min(metrics["mean_gap"] / max(metrics["std_gap"], 1e-8), 1000)

    if metrics["mean_gap"] <= 0:
        return {"decision": "reject", "reason": "negative uplift"}
    if stability < g["stability"]:
        return {"decision": "reject", "reason": "unstable feature"}
    if metrics["win_rate"] < g["win_rate"]:
        return {"decision": "reject", "reason": "low consistency"}
    if metrics["ci_lo"] <= 0:
        return {"decision": "reject", "reason": "ci crosses zero"}

    total = sum(
        _score_from_thresholds(
            stability if k == "stability" else metrics[k],
            getattr(cfg, k)["thresholds"],
            getattr(cfg, k)["scores"],
        )
        for k in ("stability", "win_rate", "ci_lo", "median_gap")
    )

    decision = cfg.decision_labels[sum(total >= g for g in cfg.decision_gates)]

    result =  {
        "decision": decision,
        "score": round(total, 2),
        "stability": round(stability, 4),
        "win_rate": round(metrics["win_rate"], 4),
        "dominance": round(metrics["dominance"], 4),
        "best_pvalue": min(metrics["p_value_ttest"], metrics["p_value_wilcoxon"]),
    }

    if decision == "reject":
        result["reason"] = "low score"
    
    return result


# ==============================================================================
# RESULTS AGGREGATION
# ==============================================================================
def aggregate_experiment_results(experiment_output_dir: str) -> tuple:
    pattern = os.path.join(experiment_output_dir, "experiment_*.parquet")
    files = sorted(
        glob.glob(pattern),
        key=lambda x: int(
            os.path.basename(x)
            .replace("experiment_", "")
            .replace(".parquet", "")
        ),
    )

    all_results = []
    for path in files:
        eid = int(
            os.path.basename(path)
            .replace("experiment_", "")
            .replace(".parquet", "")
        )
        pred_df = pd.read_parquet(path)
        base_auc = roc_auc_score(pred_df["y_true"], pred_df["baseline_proba"])
        new_auc = roc_auc_score(pred_df["y_true"], pred_df["new_feature_proba"])
        all_results.append({
            "experiment_id": eid,
            "auc_base": base_auc,
            "auc_new": new_auc,
            "gap": new_auc - base_auc,
        })

    results_df = pd.DataFrame(all_results)
    metrics = calculate_experiment_metrics(results_df)

    results_df.to_csv(
        os.path.join(experiment_output_dir, "aggregated_results.csv"),
        index=False,
    )

    print("\nFinal metrics")
    for key, value in metrics.items():
        print(f"{key}: {value:.6f}" if isinstance(value, float) else f"{key}: {value}")

    return results_df, metrics

def compute_shap_for_feature(feature_name: str, output_dir: str, cfg: NewFeatureEvalConfig) -> None:
    shap_dir = os.path.join(output_dir, "shap")
    os.makedirs(shap_dir, exist_ok=True)

    model_dir = os.path.join(output_dir, "models")
    exp_ids = sorted(
        int(f.replace("baseline_", "").replace(".cbm", ""))
        for f in os.listdir(model_dir)
        if f.startswith("baseline_") and f.endswith(".cbm")
    )

    for eid in exp_ids:
        baseline_model = CatBoostClassifier()
        baseline_model.load_model(os.path.join(model_dir, f"baseline_{eid}.cbm"))

        new_model = CatBoostClassifier()
        new_model.load_model(os.path.join(model_dir, f"new_{eid}.cbm"))

        with open(os.path.join(model_dir, f"feat_config_{eid}.json")) as f:
            feat_config = json.load(f)

        test_df = pd.read_parquet(os.path.join(model_dir, f"test_data_{eid}.parquet"))

        baseline_shap = compute_shap_values(
            baseline_model, test_df,
            feat_config["base_model_features"], feat_config["base_cat_cols"],
            cfg.shap_n_samples,
        )
        baseline_shap.to_parquet(os.path.join(shap_dir, f"baseline_shap_{eid}.parquet"), index=False)

        new_shap = compute_shap_values(
            new_model, test_df,
            feat_config["new_model_features"], feat_config["new_cat_cols"],
            cfg.shap_n_samples,
        )
        new_shap.to_parquet(os.path.join(shap_dir, f"new_shap_{eid}.parquet"), index=False)

        del baseline_model, new_model, test_df
        gc.collect()


def _aggregate_shap_output(shap_dir: str, label: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(shap_dir, f"{label}_shap_*.parquet")))
    if not files:
        return pd.DataFrame()

    count = 0
    mean_series = None
    m2_series = None

    for f in files:
        df = pd.read_parquet(f)
        exp_mean = df.abs().mean()
        count += 1

        if mean_series is None:
            mean_series = exp_mean
            m2_series = pd.Series(0.0, index=exp_mean.index)
        else:
            delta = exp_mean - mean_series
            mean_series += delta / count
            m2_series += delta * (exp_mean - mean_series)

    std_series = m2_series.div(count - 1).apply(np.sqrt).fillna(0.0)
    avg_imp = mean_series.sort_values(ascending=False)
    std_imp = std_series[avg_imp.index]

    summary = pd.DataFrame({
        "mean_abs_shap": avg_imp,
        "std_abs_shap": std_imp,
        "rank": range(1, len(avg_imp) + 1),
    })
    summary.to_csv(os.path.join(shap_dir, f"{label}_shap_importance.csv"))
    return summary


def compute_shap_for_accepted(comparison: pd.DataFrame, cfg: NewFeatureEvalConfig) -> None:
    candidates = comparison[comparison["decision"] != "reject"].copy()
    if candidates.empty:
        print("No accepted features  skipping SHAP.")
        return

    candidates = candidates.sort_values("score", ascending=False).head(cfg.top_n_for_shap)
    print(f"\n{'='*60}\nComputing SHAP for top-{len(candidates)} features:\n {candidates['feature'].tolist()}\n{'='*60}")

    for _, row in candidates.iterrows():
        feat = row["feature"]
        out_dir = row["output_dir"]
        print(f"  SHAP: {feat}")
        compute_shap_for_feature(feat, out_dir, cfg)

        shap_dir = os.path.join(out_dir, "shap")
        baseline_imp = _aggregate_shap_output(shap_dir, "baseline")
        new_imp = _aggregate_shap_output(shap_dir, "new")

        if not baseline_imp.empty and not new_imp.empty:
            shared = baseline_imp.index.intersection(new_imp.index)
            delta = pd.DataFrame({
                "baseline_mean_abs": baseline_imp.loc[shared, "mean_abs_shap"],
                "new_mean_abs": new_imp.loc[shared, "mean_abs_shap"],
                "delta": new_imp.loc[shared, "mean_abs_shap"] - baseline_imp.loc[shared, "mean_abs_shap"],
            }).sort_values("delta", ascending=False)
            delta.to_csv(os.path.join(shap_dir, "shap_delta_vs_baseline.csv"))

            print(f"\n  [{feat}] SHAP delta (new  baseline)  "
                  f"note: adding a feature redistributes importance globally, "
                  f"a drop does not mean the feature 'got worse'.")
            print(f"  {delta.to_string().replace(chr(10), chr(10)+'  ')}")

        top5 = new_imp.head(5)
        print(f"  [{feat}] Top-5 features by SHAP (new model):")
        for _name, _row in top5.iterrows():
            print(f"    {_name}: {_row['mean_abs_shap']:.6f}")


# ==============================================================================
# PIPELINE ORCHESTRATION
# ==============================================================================
def run_feature_evaluation_pipeline(
    spark: SparkSession,
    cfg: NewFeatureEvalConfig,
    new_features: Optional[list] = None,
    spark_df=None,
) -> Tuple[pd.DataFrame, dict, str]:
    if new_features is None:
        new_features = []
    new_features = [f for f in new_features if f]

    name = f"feature_{new_features[0]}" if len(new_features) == 1 else f"{len(new_features)}_new_features" if new_features else 'null_test'
    output_dir = f"{cfg.output_dir}/{name}"
    os.makedirs(output_dir, exist_ok=True)

    results_path = os.path.join(output_dir, "aggregated_results.csv")
    if os.path.exists(results_path):
        print(f"  Found cached results, loading from {output_dir}")
        results_df = pd.read_csv(results_path)
        metrics = calculate_experiment_metrics(results_df)
        return results_df, metrics, output_dir

    all_features = load_all_features(cfg)
    available = list(
        set(all_features)
        - set(cfg.removed_features + cfg.ignore_columns + new_features)
    )
    feature_samples = generate_feature_samples(
        all_features=available,
        sample_size=cfg.base_feature_sample_size,
        n_experiments=cfg.n_experiments,
        seed=cfg.seed,
    )

    if spark_df is None:
        print("Loading and joining data in Spark...")
        spark_df = load_and_join_in_spark(spark, cfg)

    experiment_configs = []
    pipe_start = time.time()

    for eid, fset in enumerate(feature_samples):
        try:
            elapsed = time.time() - pipe_start
            eta = (
                elapsed / (eid + 1) * (cfg.n_experiments - eid - 1)
                if eid > 0
                else 0
            )
            print(f"\nExperiment {eid + 1}/{cfg.n_experiments}  (ETA: {eta/60:.1f}min)")

            run_single_experiment(
                experiment_id=eid,
                base_feature_cols=fset,
                new_feature_cols=new_features,
                spark_df=spark_df,
                experiment_output_dir=output_dir,
                cfg=cfg,
            )

            experiment_configs.append({
                "experiment_id": eid,
                "n_base_features": len(fset),
                "new_feature_cols": new_features,
                "base_feature_cols": fset,
            })
        except Exception as error:
            print(f"Experiment {eid} failed: {error}")
        gc.collect()

    global_config = {
        "split_date": cfg.split_date,
        "base_feature_sample_size": cfg.base_feature_sample_size,
        "n_experiments": cfg.n_experiments,
        "new_features": new_features,
        "target_col": cfg.target_col,
        "base_features_path": cfg.base_features_path,
        "new_features_path": cfg.new_features_path,
        "output_dir": cfg.output_dir,
        "n_removed_features": len(cfg.removed_features),
        "ignore_columns": cfg.ignore_columns,
        "join_keys": cfg.join_keys,
        "n_all_features": len(all_features),
        "n_available_features": len(available),
        "catboost_params": cfg.catboost_params,
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
    }
    save_experiment_config(output_dir, global_config, experiment_configs)

    results, metrics = aggregate_experiment_results(output_dir)
    return results, metrics, output_dir

def evaluate_features_in_bulk(
    spark: SparkSession,
    cfg: NewFeatureEvalConfig,
    new_features_list: Optional[list] = None,
) -> pd.DataFrame:
    if new_features_list is None:
        all_cols = sorted(spark.read.parquet(cfg.new_features_path).columns)
        new_features_list = [
            c for c in all_cols
            if c not in cfg.ignore_columns + ["date"] + cfg.removed_features
        ]

    if cfg.n_experiments < 30:
        print(f"  n_experiments={cfg.n_experiments} < 30. "
              f"t-test/Wilcoxon have low power; trust bootstrap CI (ci_lo, ci_hi) instead.")

    print("Loading and joining data in Spark (single pass for all features)...")
    spark_df = load_and_join_in_spark(spark, cfg)

    rows = []
    start_time = time.time()

    for i, feat in enumerate(new_features_list):
        print(f"\n{'='*60}\nEvaluating feature [{i+1}/{len(new_features_list)}]: {feat}\n{'='*60}")
        results, metrics, output_dir = run_feature_evaluation_pipeline(
            spark, cfg, new_features=[feat], spark_df=spark_df,
        )
        decision = evaluate_feature(metrics, cfg)
        rows.append({"feature": feat, "output_dir": output_dir, **metrics, **decision})

        elapsed = time.time() - start_time
        eta = (
            elapsed / (i + 1) * (len(new_features_list) - i - 1)
            if i > 0
            else 0
        )
        print(f"[{i+1}/{len(new_features_list)}] {feat}  (ETA: {eta/60:.1f}min)")

    spark_df.unpersist()

    comparison = pd.DataFrame(rows)
    if comparison.empty:
        print("No features evaluated  returning empty comparison.")
        return comparison

    comparison["decision"] = pd.Categorical(
        comparison["decision"],
        categories=cfg.decision_labels,
        ordered=True,
    )
    comparison = comparison.sort_values("decision", ascending=False)
    print("\n\nComparison:\n", comparison)

    if cfg.top_n_for_shap > 0:
        compute_shap_for_accepted(comparison, cfg)

    return comparison

def evaluate_features_in_bulk(
    spark: SparkSession,
    cfg: NewFeatureEvalConfig,
    new_features_list: Optional[list] = None,
) -> pd.DataFrame:
    if new_features_list is None:
        all_cols = sorted(spark.read.parquet(cfg.new_features_path).columns)
        new_features_list = [
            c for c in all_cols
            if c not in cfg.ignore_columns + ["date"] + cfg.removed_features
        ]

    if cfg.n_experiments < 30:
        print(f"  n_experiments={cfg.n_experiments} < 30. "
              f"t-test/Wilcoxon have low power; trust bootstrap CI (ci_lo, ci_hi) instead.")

    print("Loading and joining data in Spark (single pass for all features)...")
    spark_df = load_and_join_in_spark(spark, cfg)

    rows = []
    start_time = time.time()

    for i, feat in enumerate(new_features_list):
        print(f"\n{'='*60}\nEvaluating feature [{i+1}/{len(new_features_list)}]: {feat}\n{'='*60}")
        results, metrics, output_dir = run_feature_evaluation_pipeline(
            spark, cfg, new_features=[feat], spark_df=spark_df,
        )
        decision = evaluate_feature(metrics, cfg)
        rows.append({"feature": feat, "output_dir": output_dir, **metrics, **decision})

        elapsed = time.time() - start_time
        eta = (
            elapsed / (i + 1) * (len(new_features_list) - i - 1)
            if i > 0
            else 0
        )
        print(f"[{i+1}/{len(new_features_list)}] {feat}  (ETA: {eta/60:.1f}min)")

    spark_df.unpersist()

    comparison = pd.DataFrame(rows)
    if comparison.empty:
        print("No features evaluated  returning empty comparison.")
        return comparison

    comparison["decision"] = pd.Categorical(
        comparison["decision"],
        categories=cfg.decision_labels,
        ordered=True,
    )
    comparison = comparison.sort_values("decision", ascending=False)
    print("\n\nComparison:\n", comparison)

    if cfg.top_n_for_shap > 0:
        compute_shap_for_accepted(comparison, cfg)

    return comparison