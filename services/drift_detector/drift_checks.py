"""
DRIFT DETECTOR - Drift Checks using Evidently
==============================================

Uses Evidently library to detect feature drift and prediction drift.

WHAT IS EVIDENTLY?
------------------
Evidently is an open-source ML monitoring library that can:
1. Detect data drift (features changed)
2. Detect prediction drift (model outputs changed)
3. Generate beautiful HTML reports
4. Calculate statistical tests

HOW DRIFT DETECTION WORKS:
--------------------------
For each feature, Evidently runs a statistical test:

1. For numerical features: Kolmogorov-Smirnov test
   - Compares the cumulative distributions
   - Returns p-value (probability distributions are same)
   - p-value < 0.05 → Drift detected!

2. For categorical features: Chi-squared test
   - Compares category frequencies
   - Returns p-value
   - p-value < 0.05 → Drift detected!

Example:
- Reference: amount mean = $150, std = $50
- Current:   amount mean = $280, std = $80
- KS test p-value = 0.001 → DRIFT DETECTED!
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset
from evidently.metrics import (
    DataDriftTable,
    DatasetDriftMetric,
    ColumnDriftMetric,
    ColumnSummaryMetric,
)

from config import config

log = logging.getLogger("drift_detector.checks")


@dataclass
class FeatureDriftResult:
    """Result for a single feature's drift check."""
    feature_name: str
    drift_detected: bool
    drift_score: float  # p-value or similar
    stattest_name: str
    threshold: float
    reference_mean: Optional[float] = None
    reference_std: Optional[float] = None
    current_mean: Optional[float] = None
    current_std: Optional[float] = None


@dataclass
class DriftCheckResult:
    """Complete drift check results."""
    timestamp: datetime
    dataset_drift_detected: bool
    dataset_drift_share: float  # Fraction of features with drift
    num_features_checked: int
    num_features_drifted: int
    feature_results: List[FeatureDriftResult] = field(default_factory=list)
    reference_rows: int = 0
    current_rows: int = 0
    error: Optional[str] = None
    html_report_path: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "dataset_drift_detected": self.dataset_drift_detected,
            "dataset_drift_share": round(self.dataset_drift_share, 4),
            "num_features_checked": self.num_features_checked,
            "num_features_drifted": self.num_features_drifted,
            "reference_rows": self.reference_rows,
            "current_rows": self.current_rows,
            "drifted_features": [
                f.feature_name for f in self.feature_results if f.drift_detected
            ],
            "feature_details": [
                {
                    "feature": f.feature_name,
                    "drift_detected": f.drift_detected,
                    "drift_score": round(f.drift_score, 6),
                    "stattest": f.stattest_name,
                    "reference_mean": round(f.reference_mean, 4) if f.reference_mean else None,
                    "current_mean": round(f.current_mean, 4) if f.current_mean else None,
                }
                for f in self.feature_results
            ],
            "error": self.error,
        }


def run_drift_check(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    features: List[str] = None,
    generate_report: bool = True,
    report_path: str = "/tmp/drift_report.html",
) -> DriftCheckResult:
    """
    Run drift detection comparing reference and current data.
    
    Args:
        reference_df: Historical/training data (what "normal" looks like)
        current_df: Recent production data (what's happening now)
        features: List of features to check (default from config)
        generate_report: Whether to generate HTML report
        report_path: Where to save HTML report
    
    Returns:
        DriftCheckResult with all drift information
    """
    timestamp = datetime.now()
    
    if features is None:
        features = config.drift.monitored_features
    
    # Filter to only features that exist in both dataframes
    available_features = [
        f for f in features 
        if f in reference_df.columns and f in current_df.columns
    ]
    
    if not available_features:
        return DriftCheckResult(
            timestamp=timestamp,
            dataset_drift_detected=False,
            dataset_drift_share=0.0,
            num_features_checked=0,
            num_features_drifted=0,
            error="No features available for drift detection",
        )
    
    log.info(f"Running drift check on {len(available_features)} features")
    log.info(f"Reference: {len(reference_df)} rows, Current: {len(current_df)} rows")
    
    # Prepare data for Evidently
    reference_subset = reference_df[available_features].copy()
    current_subset = current_df[available_features].copy()
    
    # Handle any NaN values
    reference_subset = reference_subset.fillna(0)
    current_subset = current_subset.fillna(0)
    
    try:
        # Create Evidently report with drift metrics
        report = Report(metrics=[
            DatasetDriftMetric(),
            DataDriftTable(),
        ])
        
        # Run the report
        report.run(
            reference_data=reference_subset,
            current_data=current_subset,
        )
        
        # Extract results
        report_dict = report.as_dict()
        
        # Get dataset-level drift
        dataset_metrics = report_dict["metrics"][0]["result"]
        dataset_drift = dataset_metrics.get("dataset_drift", False)
        drift_share = dataset_metrics.get("drift_share", 0.0)
        
        # Get per-feature drift
        drift_table = report_dict["metrics"][1]["result"]
        drift_by_columns = drift_table.get("drift_by_columns", {})
        
        feature_results = []
        num_drifted = 0
        
        for feature_name in available_features:
            if feature_name in drift_by_columns:
                col_info = drift_by_columns[feature_name]
                is_drifted = col_info.get("drift_detected", False)
                drift_score = col_info.get("drift_score", 1.0)
                stattest = col_info.get("stattest_name", "unknown")
                threshold = col_info.get("stattest_threshold", 0.05)
                
                # Get distribution stats
                ref_mean = reference_subset[feature_name].mean()
                ref_std = reference_subset[feature_name].std()
                cur_mean = current_subset[feature_name].mean()
                cur_std = current_subset[feature_name].std()
                
                if is_drifted:
                    num_drifted += 1
                    log.warning(
                        f"DRIFT DETECTED: {feature_name} "
                        f"(score={drift_score:.6f}, ref_mean={ref_mean:.2f}, cur_mean={cur_mean:.2f})"
                    )
                
                feature_results.append(FeatureDriftResult(
                    feature_name=feature_name,
                    drift_detected=is_drifted,
                    drift_score=drift_score,
                    stattest_name=stattest,
                    threshold=threshold,
                    reference_mean=ref_mean,
                    reference_std=ref_std,
                    current_mean=cur_mean,
                    current_std=cur_std,
                ))
        
        # Generate HTML report if requested
        html_path = None
        if generate_report:
            try:
                report.save_html(report_path)
                html_path = report_path
                log.info(f"Saved HTML report to {report_path}")
            except Exception as e:
                log.warning(f"Failed to save HTML report: {e}")
        
        result = DriftCheckResult(
            timestamp=timestamp,
            dataset_drift_detected=dataset_drift,
            dataset_drift_share=drift_share,
            num_features_checked=len(available_features),
            num_features_drifted=num_drifted,
            feature_results=feature_results,
            reference_rows=len(reference_df),
            current_rows=len(current_df),
            html_report_path=html_path,
        )
        
        log.info(
            f"Drift check complete: drift={dataset_drift}, "
            f"share={drift_share:.2%}, features_drifted={num_drifted}/{len(available_features)}"
        )
        
        return result
        
    except Exception as e:
        log.exception(f"Drift check failed: {e}")
        return DriftCheckResult(
            timestamp=timestamp,
            dataset_drift_detected=False,
            dataset_drift_share=0.0,
            num_features_checked=len(available_features),
            num_features_drifted=0,
            reference_rows=len(reference_df),
            current_rows=len(current_df),
            error=str(e),
        )


def calculate_simple_drift_metrics(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    features: List[str] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Calculate simple drift metrics without Evidently.
    
    Useful for quick checks or when Evidently has issues.
    
    Returns dictionary with per-feature statistics:
    - mean_diff: Difference in means
    - std_diff: Difference in standard deviations
    - psi: Population Stability Index
    """
    if features is None:
        features = config.drift.monitored_features
    
    results = {}
    
    for feature in features:
        if feature not in reference_df.columns or feature not in current_df.columns:
            continue
        
        ref = reference_df[feature].dropna()
        cur = current_df[feature].dropna()
        
        if len(ref) == 0 or len(cur) == 0:
            continue
        
        # Basic statistics
        ref_mean = ref.mean()
        cur_mean = cur.mean()
        ref_std = ref.std()
        cur_std = cur.std()
        
        # Population Stability Index (PSI)
        # PSI > 0.1 suggests moderate drift
        # PSI > 0.25 suggests significant drift
        try:
            psi = calculate_psi(ref, cur)
        except:
            psi = 0.0
        
        results[feature] = {
            "reference_mean": ref_mean,
            "current_mean": cur_mean,
            "mean_diff": cur_mean - ref_mean,
            "mean_diff_pct": (cur_mean - ref_mean) / ref_mean * 100 if ref_mean != 0 else 0,
            "reference_std": ref_std,
            "current_std": cur_std,
            "psi": psi,
            "drift_detected": psi > 0.1,
        }
    
    return results


def calculate_psi(reference: pd.Series, current: pd.Series, bins: int = 10) -> float:
    """
    Calculate Population Stability Index (PSI).
    
    PSI measures how different two distributions are:
    - PSI < 0.1: No significant change
    - 0.1 <= PSI < 0.25: Moderate change
    - PSI >= 0.25: Significant change
    
    Formula: PSI = sum((current% - reference%) * ln(current% / reference%))
    """
    # Create bins based on reference distribution
    _, bin_edges = np.histogram(reference, bins=bins)
    
    # Calculate percentages in each bin
    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current, bins=bin_edges)
    
    ref_pct = ref_counts / len(reference)
    cur_pct = cur_counts / len(current)
    
    # Avoid division by zero
    ref_pct = np.where(ref_pct == 0, 0.0001, ref_pct)
    cur_pct = np.where(cur_pct == 0, 0.0001, cur_pct)
    
    # Calculate PSI
    psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    
    return float(psi)
