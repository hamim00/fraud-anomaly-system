"""
EVALUATION METRICS - Metrics for fraud detection models
================================================================================

For fraud detection, we care about different metrics than typical classification:

1. PR-AUC (Precision-Recall AUC)
   - Better than ROC-AUC for imbalanced data
   - Focuses on the minority class (fraud)

2. Recall at Fixed FPR
   - "How many frauds can we catch if we only allow X% false alarms?"
   - Business-critical: false positives cost money (blocked legitimate customers)

3. Precision at Fixed Recall
   - "Of our alerts, how many are actually fraud?"
   - Important for fraud investigation team capacity

================================================================================
"""

import logging
from typing import Dict, Any, Tuple, Optional
import numpy as np
from sklearn.metrics import (
    precision_recall_curve,
    roc_curve,
    auc,
    roc_auc_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report,
)

log = logging.getLogger("trainer.evaluate")


def calculate_metrics(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float = 0.5
) -> Dict[str, float]:
    """
    Calculate comprehensive metrics for fraud detection.
    
    Args:
        y_true: True labels (0 or 1)
        y_pred_proba: Predicted probabilities for class 1 (fraud)
        threshold: Decision threshold (default 0.5)
    
    Returns:
        Dictionary of metrics
    """
    y_pred = (y_pred_proba >= threshold).astype(int)
    
    metrics = {}
    
    # =========================================================================
    # PR-AUC (Primary metric for imbalanced data)
    # =========================================================================
    precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_pred_proba)
    metrics["pr_auc"] = auc(recall_curve, precision_curve)
    
    # =========================================================================
    # ROC-AUC
    # =========================================================================
    metrics["roc_auc"] = roc_auc_score(y_true, y_pred_proba)
    
    # =========================================================================
    # Recall at various FPR thresholds
    # "How much fraud can we catch at X% false positive rate?"
    # =========================================================================
    fpr, tpr, roc_thresholds = roc_curve(y_true, y_pred_proba)
    
    # Recall at 1% FPR
    metrics["recall_at_1pct_fpr"] = float(tpr[fpr <= 0.01].max()) if any(fpr <= 0.01) else 0.0
    
    # Recall at 5% FPR
    metrics["recall_at_5pct_fpr"] = float(tpr[fpr <= 0.05].max()) if any(fpr <= 0.05) else 0.0
    
    # Recall at 10% FPR
    metrics["recall_at_10pct_fpr"] = float(tpr[fpr <= 0.10].max()) if any(fpr <= 0.10) else 0.0
    
    # =========================================================================
    # Threshold-based metrics
    # =========================================================================
    metrics["precision"] = precision_score(y_true, y_pred, zero_division=0)
    metrics["recall"] = recall_score(y_true, y_pred, zero_division=0)
    metrics["f1"] = f1_score(y_true, y_pred, zero_division=0)
    
    # Confusion matrix values
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    metrics["true_positives"] = int(tp)
    metrics["false_positives"] = int(fp)
    metrics["true_negatives"] = int(tn)
    metrics["false_negatives"] = int(fn)
    
    # =========================================================================
    # Business metrics
    # =========================================================================
    # False positive rate (legitimate customers blocked)
    metrics["fpr"] = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    
    # False negative rate (fraud missed)
    metrics["fnr"] = fn / (fn + tp) if (fn + tp) > 0 else 0.0
    
    return metrics


def find_optimal_threshold(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    target_fpr: float = 0.05
) -> Tuple[float, Dict[str, float]]:
    """
    Find the threshold that achieves a target false positive rate.
    
    This is useful for business constraints like:
    "We can only afford to block 5% of legitimate customers"
    
    Args:
        y_true: True labels
        y_pred_proba: Predicted probabilities
        target_fpr: Target false positive rate
    
    Returns:
        Tuple of (optimal_threshold, metrics_at_threshold)
    """
    fpr, tpr, thresholds = roc_curve(y_true, y_pred_proba)
    
    # Find threshold closest to target FPR
    idx = np.argmin(np.abs(fpr - target_fpr))
    optimal_threshold = thresholds[idx]
    
    # Calculate metrics at this threshold
    metrics = calculate_metrics(y_true, y_pred_proba, threshold=optimal_threshold)
    metrics["optimal_threshold"] = optimal_threshold
    metrics["actual_fpr"] = fpr[idx]
    metrics["actual_tpr"] = tpr[idx]
    
    return optimal_threshold, metrics


def calculate_lift(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    top_k_pct: float = 0.10
) -> float:
    """
    Calculate lift for top K% of predictions.
    
    Lift = (fraud rate in top K%) / (overall fraud rate)
    
    A lift of 5 means: If we investigate the top 10% of scores,
    we find 5x more fraud than random sampling.
    
    Args:
        y_true: True labels
        y_pred_proba: Predicted probabilities
        top_k_pct: Top percentage to consider (default 10%)
    
    Returns:
        Lift value
    """
    n = len(y_true)
    k = int(n * top_k_pct)
    
    # Get indices of top K predictions
    top_k_indices = np.argsort(y_pred_proba)[-k:]
    
    # Fraud rate in top K
    fraud_rate_top_k = y_true.iloc[top_k_indices].mean() if hasattr(y_true, 'iloc') else y_true[top_k_indices].mean()
    
    # Overall fraud rate
    overall_fraud_rate = y_true.mean()
    
    if overall_fraud_rate == 0:
        return 0.0
    
    lift = fraud_rate_top_k / overall_fraud_rate
    return lift


def print_evaluation_report(
    y_true: np.ndarray,
    y_pred_proba: np.ndarray,
    threshold: float = 0.5,
    dataset_name: str = "Test"
) -> Dict[str, float]:
    """
    Print a comprehensive evaluation report and return metrics.
    
    Args:
        y_true: True labels
        y_pred_proba: Predicted probabilities
        threshold: Decision threshold
        dataset_name: Name for logging (e.g., "Test", "Validation")
    
    Returns:
        Dictionary of metrics
    """
    metrics = calculate_metrics(y_true, y_pred_proba, threshold)
    
    log.info("=" * 60)
    log.info(f"EVALUATION REPORT: {dataset_name} Set")
    log.info("=" * 60)
    
    log.info("\n📊 PRIMARY METRICS (for imbalanced data):")
    log.info(f"   PR-AUC:            {metrics['pr_auc']:.4f}")
    log.info(f"   ROC-AUC:           {metrics['roc_auc']:.4f}")
    
    log.info("\n🎯 RECALL AT FIXED FPR (business-critical):")
    log.info(f"   Recall @ 1% FPR:   {metrics['recall_at_1pct_fpr']:.4f}")
    log.info(f"   Recall @ 5% FPR:   {metrics['recall_at_5pct_fpr']:.4f}")
    log.info(f"   Recall @ 10% FPR:  {metrics['recall_at_10pct_fpr']:.4f}")
    
    log.info(f"\n📋 AT THRESHOLD = {threshold}:")
    log.info(f"   Precision:         {metrics['precision']:.4f}")
    log.info(f"   Recall:            {metrics['recall']:.4f}")
    log.info(f"   F1 Score:          {metrics['f1']:.4f}")
    
    log.info("\n🔢 CONFUSION MATRIX:")
    log.info(f"   True Positives:    {metrics['true_positives']}")
    log.info(f"   False Positives:   {metrics['false_positives']}")
    log.info(f"   True Negatives:    {metrics['true_negatives']}")
    log.info(f"   False Negatives:   {metrics['false_negatives']}")
    
    # Calculate lift
    lift_10pct = calculate_lift(y_true, y_pred_proba, top_k_pct=0.10)
    log.info(f"\n📈 LIFT @ Top 10%:    {lift_10pct:.2f}x")
    
    log.info("=" * 60)
    
    return metrics


def check_promotion_criteria(
    metrics: Dict[str, float],
    min_pr_auc: float = 0.20,
    min_recall_at_5pct_fpr: float = 0.40
) -> Tuple[bool, str]:
    """
    Check if model meets promotion criteria.
    
    Args:
        metrics: Dictionary of metrics
        min_pr_auc: Minimum PR-AUC required
        min_recall_at_5pct_fpr: Minimum recall at 5% FPR required
    
    Returns:
        Tuple of (should_promote, reason)
    """
    reasons = []
    
    if metrics["pr_auc"] < min_pr_auc:
        reasons.append(f"PR-AUC {metrics['pr_auc']:.4f} < {min_pr_auc}")
    
    if metrics["recall_at_5pct_fpr"] < min_recall_at_5pct_fpr:
        reasons.append(f"Recall@5%FPR {metrics['recall_at_5pct_fpr']:.4f} < {min_recall_at_5pct_fpr}")
    
    if reasons:
        return False, "Not promoted: " + "; ".join(reasons)
    else:
        return True, f"✅ Promoted! PR-AUC={metrics['pr_auc']:.4f}, Recall@5%FPR={metrics['recall_at_5pct_fpr']:.4f}"
