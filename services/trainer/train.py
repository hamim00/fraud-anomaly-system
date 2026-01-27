"""
TRAINING SCRIPT - Train fraud detection models with MLflow tracking
================================================================================

This script:
1. Loads features from PostgreSQL
2. Splits data by TIME (not random!)
3. Trains XGBoost (supervised) + Isolation Forest (unsupervised)
4. Logs everything to MLflow
5. Promotes model if it meets quality thresholds

USAGE:
------
    # Run locally
    python train.py
    
    # Run in Docker
    docker compose run trainer

================================================================================
"""

import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
from xgboost import XGBClassifier

from config import config
from data_loader import (
    load_features,
    time_based_split,
    prepare_features,
    compute_data_hash,
    get_feature_stats,
)
from evaluate import (
    calculate_metrics,
    print_evaluation_report,
    check_promotion_criteria,
    find_optimal_threshold,
)


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("trainer")


def train_xgboost(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    params: Dict[str, Any]
) -> XGBClassifier:
    """
    Train XGBoost classifier with class imbalance handling.
    
    Args:
        X_train, y_train: Training data
        X_val, y_val: Validation data (for early stopping)
        params: XGBoost parameters
    
    Returns:
        Trained XGBClassifier
    """
    log.info("Training XGBoost classifier...")
    
    # Calculate scale_pos_weight for class imbalance
    # This tells XGBoost to pay more attention to the minority class (fraud)
    n_negative = (y_train == 0).sum()
    n_positive = (y_train == 1).sum()
    scale_pos_weight = n_negative / n_positive if n_positive > 0 else 1
    
    log.info(f"Class balance: {n_negative:,} negative, {n_positive:,} positive")
    log.info(f"scale_pos_weight: {scale_pos_weight:.2f}")
    
    model = XGBClassifier(
        **params,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",  # Optimize for PR-AUC
        early_stopping_rounds=10,
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )
    
    log.info(f"XGBoost trained. Best iteration: {model.best_iteration}")
    
    return model


def train_isolation_forest(
    X_train: pd.DataFrame,
    params: Dict[str, Any]
) -> IsolationForest:
    """
    Train Isolation Forest for unsupervised anomaly detection.
    
    Isolation Forest doesn't use labels - it learns what's "normal"
    and flags outliers. Good for detecting novel fraud patterns
    that weren't in the training data.
    
    Args:
        X_train: Training features (labels not used)
        params: Isolation Forest parameters
    
    Returns:
        Trained IsolationForest
    """
    log.info("Training Isolation Forest (unsupervised)...")
    
    model = IsolationForest(**params)
    model.fit(X_train)
    
    log.info("Isolation Forest trained.")
    
    return model


def get_feature_importance(
    model: XGBClassifier,
    feature_names: list
) -> pd.DataFrame:
    """Get feature importance from XGBoost model."""
    importance = model.feature_importances_
    
    df = pd.DataFrame({
        "feature": feature_names,
        "importance": importance
    }).sort_values("importance", ascending=False)
    
    return df


def main():
    """Main training pipeline."""
    
    log.info("=" * 70)
    log.info("FRAUD DETECTION MODEL TRAINING")
    log.info("=" * 70)
    
    # =========================================================================
    # SETUP MLFLOW
    # =========================================================================
    log.info(f"MLflow tracking URI: {config.mlflow.tracking_uri}")
    mlflow.set_tracking_uri(config.mlflow.tracking_uri)
    mlflow.set_experiment(config.mlflow.experiment_name)
    
    # =========================================================================
    # LOAD DATA
    # =========================================================================
    log.info("\n📥 LOADING DATA...")
    df = load_features(min_rows=500)  # Require at least 500 rows
    data_hash = compute_data_hash(df)
    data_stats = get_feature_stats(df)
    
    # =========================================================================
    # TIME-BASED SPLIT
    # =========================================================================
    log.info("\n✂️ SPLITTING DATA (by time)...")
    train_df, val_df, test_df = time_based_split(
        df,
        train_ratio=config.training.train_ratio,
        val_ratio=config.training.val_ratio,
        test_ratio=config.training.test_ratio,
    )
    
    # =========================================================================
    # PREPARE FEATURES
    # =========================================================================
    log.info("\n🔧 PREPARING FEATURES...")
    X_train, y_train = prepare_features(
        train_df, 
        config.training.feature_columns,
        config.training.target_column
    )
    X_val, y_val = prepare_features(
        val_df,
        config.training.feature_columns,
        config.training.target_column
    )
    X_test, y_test = prepare_features(
        test_df,
        config.training.feature_columns,
        config.training.target_column
    )
    
    feature_names = list(X_train.columns)
    
    # =========================================================================
    # START MLFLOW RUN
    # =========================================================================
    with mlflow.start_run(run_name=f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        
        # Log data info
        mlflow.log_param("data_hash", data_hash)
        mlflow.log_param("train_size", len(train_df))
        mlflow.log_param("val_size", len(val_df))
        mlflow.log_param("test_size", len(test_df))
        mlflow.log_param("total_features", len(feature_names))
        mlflow.log_param("feature_names", str(feature_names))
        
        mlflow.log_param("train_fraud_rate", float(y_train.mean()))
        mlflow.log_param("val_fraud_rate", float(y_val.mean()))
        mlflow.log_param("test_fraud_rate", float(y_test.mean()))
        
        # Log training config
        for key, value in config.training.xgb_params.items():
            mlflow.log_param(f"xgb_{key}", value)
        
        # =================================================================
        # TRAIN XGBOOST
        # =================================================================
        log.info("\n🚀 TRAINING XGBOOST...")
        xgb_model = train_xgboost(
            X_train, y_train,
            X_val, y_val,
            config.training.xgb_params
        )
        
        # Get predictions
        y_train_pred = xgb_model.predict_proba(X_train)[:, 1]
        y_val_pred = xgb_model.predict_proba(X_val)[:, 1]
        y_test_pred = xgb_model.predict_proba(X_test)[:, 1]
        
        # Evaluate
        log.info("\n📊 XGBOOST EVALUATION:")
        train_metrics = print_evaluation_report(y_train, y_train_pred, dataset_name="Train")
        val_metrics = print_evaluation_report(y_val, y_val_pred, dataset_name="Validation")
        test_metrics = print_evaluation_report(y_test, y_test_pred, dataset_name="Test")
        
        # Log metrics to MLflow
        for name, value in test_metrics.items():
            if isinstance(value, (int, float)):
                mlflow.log_metric(f"test_{name}", value)
        
        for name, value in val_metrics.items():
            if isinstance(value, (int, float)):
                mlflow.log_metric(f"val_{name}", value)
        
        # Feature importance
        importance_df = get_feature_importance(xgb_model, feature_names)
        log.info("\n📋 TOP 10 FEATURE IMPORTANCE:")
        for _, row in importance_df.head(10).iterrows():
            log.info(f"   {row['feature']}: {row['importance']:.4f}")
        
        # Log feature importance as artifact
        importance_df.to_csv("/tmp/feature_importance.csv", index=False)
        mlflow.log_artifact("/tmp/feature_importance.csv")
        
        # =================================================================
        # TRAIN ISOLATION FOREST
        # =================================================================
        log.info("\n🌲 TRAINING ISOLATION FOREST (unsupervised)...")
        iforest_model = train_isolation_forest(X_train, config.training.iforest_params)
        
        # Isolation Forest returns -1 for anomalies, 1 for normal
        # Convert to anomaly scores (higher = more anomalous)
        iforest_scores_test = -iforest_model.score_samples(X_test)
        
        # Normalize to 0-1 range for comparison
        iforest_scores_test_norm = (iforest_scores_test - iforest_scores_test.min()) / (iforest_scores_test.max() - iforest_scores_test.min())
        
        # Evaluate Isolation Forest
        iforest_metrics = calculate_metrics(y_test.values, iforest_scores_test_norm)
        log.info(f"\n📊 ISOLATION FOREST (Test):")
        log.info(f"   PR-AUC:            {iforest_metrics['pr_auc']:.4f}")
        log.info(f"   ROC-AUC:           {iforest_metrics['roc_auc']:.4f}")
        log.info(f"   Recall @ 5% FPR:   {iforest_metrics['recall_at_5pct_fpr']:.4f}")
        
        mlflow.log_metric("iforest_pr_auc", iforest_metrics["pr_auc"])
        mlflow.log_metric("iforest_roc_auc", iforest_metrics["roc_auc"])
        
        # =================================================================
        # FIND OPTIMAL THRESHOLD
        # =================================================================
        log.info("\n🎯 FINDING OPTIMAL THRESHOLD...")
        optimal_threshold, threshold_metrics = find_optimal_threshold(
            y_test.values, y_test_pred, target_fpr=0.05
        )
        log.info(f"   Optimal threshold for 5% FPR: {optimal_threshold:.4f}")
        log.info(f"   Recall at this threshold: {threshold_metrics['actual_tpr']:.4f}")
        
        mlflow.log_param("optimal_threshold", optimal_threshold)
        mlflow.log_metric("recall_at_optimal_threshold", threshold_metrics["actual_tpr"])
        
        # =================================================================
        # LOG MODELS
        # =================================================================
        log.info("\n💾 SAVING MODELS TO MLFLOW...")
        
        mlflow.sklearn.log_model(
            xgb_model, 
            "xgboost_model",
            registered_model_name="fraud-detector-xgboost"
        )
        
        mlflow.sklearn.log_model(
            iforest_model,
            "isolation_forest_model",
            registered_model_name="fraud-detector-iforest"
        )
        
        # =================================================================
        # PROMOTION CHECK
        # =================================================================
        log.info("\n🏆 CHECKING PROMOTION CRITERIA...")
        should_promote, reason = check_promotion_criteria(
            test_metrics,
            min_pr_auc=config.training.min_pr_auc,
            min_recall_at_5pct_fpr=config.training.min_recall_at_5pct_fpr,
        )
        
        log.info(reason)
        mlflow.log_param("promoted", should_promote)
        mlflow.set_tag("promotion_status", "promoted" if should_promote else "not_promoted")
        
        if should_promote:
            mlflow.set_tag("model_stage", "production_candidate")
            log.info("🎉 Model meets quality thresholds and is a production candidate!")
        else:
            mlflow.set_tag("model_stage", "experimental")
            log.info("⚠️ Model does not meet quality thresholds. Needs improvement.")
        
        # =================================================================
        # SUMMARY
        # =================================================================
        log.info("\n" + "=" * 70)
        log.info("TRAINING COMPLETE!")
        log.info("=" * 70)
        log.info(f"📊 Test PR-AUC:           {test_metrics['pr_auc']:.4f}")
        log.info(f"📊 Test ROC-AUC:          {test_metrics['roc_auc']:.4f}")
        log.info(f"📊 Test Recall @ 5% FPR:  {test_metrics['recall_at_5pct_fpr']:.4f}")
        log.info(f"🎯 Optimal Threshold:     {optimal_threshold:.4f}")
        log.info(f"🏆 Promotion Status:      {'✅ PROMOTED' if should_promote else '❌ NOT PROMOTED'}")
        log.info(f"🔗 MLflow Run ID:         {mlflow.active_run().info.run_id}")
        log.info("=" * 70)
        
        return {
            "run_id": mlflow.active_run().info.run_id,
            "test_metrics": test_metrics,
            "promoted": should_promote,
            "optimal_threshold": optimal_threshold,
        }


if __name__ == "__main__":
    try:
        result = main()
        log.info(f"Training finished successfully. Run ID: {result['run_id']}")
    except Exception as e:
        log.exception(f"Training failed: {e}")
        sys.exit(1)
