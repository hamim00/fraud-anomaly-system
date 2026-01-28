"""
DRIFT DETECTOR SERVICE - Main Application
==========================================

This service monitors your fraud detection system for data drift.

WHAT IT DOES:
-------------
1. Runs drift checks every N minutes (configurable)
2. Compares current data to reference (training) data
3. Exposes metrics to Prometheus for Grafana dashboards
4. Provides API for manual drift checks and reports

ENDPOINTS:
----------
GET  /health        - Health check
GET  /drift/status  - Current drift status
GET  /drift/run     - Trigger manual drift check
GET  /drift/report  - Download HTML report
GET  /drift/stats   - Data statistics
GET  /metrics       - Prometheus metrics

HOW TO USE:
-----------
1. Service runs automatically, checking every 5 minutes
2. View drift status in Grafana dashboard
3. Get alerts when drift is detected
4. Download detailed HTML reports for investigation

==========================================
"""

import logging
import sys
import os
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from prometheus_client import (
    Counter, Gauge, Histogram, Info,
    generate_latest, CONTENT_TYPE_LATEST,
    start_http_server,
)
from apscheduler.schedulers.background import BackgroundScheduler

from config import config
from reference_data import (
    load_reference_and_current,
    get_data_stats,
)
from drift_checks import (
    run_drift_check,
    DriftCheckResult,
    calculate_simple_drift_metrics,
)

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=getattr(logging, config.server.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("drift_detector")


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

# Overall drift status
DRIFT_DETECTED = Gauge(
    'fraud_drift_detected',
    'Whether drift is currently detected (1=yes, 0=no)'
)

DRIFT_SHARE = Gauge(
    'fraud_drift_share',
    'Fraction of features with detected drift (0-1)'
)

DRIFT_FEATURES_TOTAL = Gauge(
    'fraud_drift_features_total',
    'Total number of features monitored for drift'
)

DRIFT_FEATURES_DRIFTED = Gauge(
    'fraud_drift_features_drifted',
    'Number of features with detected drift'
)

# Per-feature drift scores
DRIFT_SCORE = Gauge(
    'fraud_drift_score',
    'Drift score (p-value) for each feature',
    ['feature']
)

FEATURE_DRIFT_DETECTED = Gauge(
    'fraud_feature_drift_detected',
    'Whether drift is detected for specific feature (1=yes, 0=no)',
    ['feature']
)

# Feature statistics
FEATURE_MEAN = Gauge(
    'fraud_feature_mean',
    'Mean value of feature',
    ['feature', 'dataset']  # dataset = 'reference' or 'current'
)

# Drift check execution
DRIFT_CHECK_DURATION = Histogram(
    'fraud_drift_check_duration_seconds',
    'Time taken to run drift check',
    buckets=[1, 5, 10, 30, 60, 120, 300]
)

DRIFT_CHECK_ERRORS = Counter(
    'fraud_drift_check_errors_total',
    'Total number of drift check errors'
)

DRIFT_CHECKS_RUN = Counter(
    'fraud_drift_checks_run_total',
    'Total number of drift checks run'
)

LAST_CHECK_TIME = Gauge(
    'fraud_drift_last_check_timestamp',
    'Unix timestamp of last drift check'
)

# Data availability
DATA_REFERENCE_ROWS = Gauge(
    'fraud_drift_reference_rows',
    'Number of rows in reference dataset'
)

DATA_CURRENT_ROWS = Gauge(
    'fraud_drift_current_rows',
    'Number of rows in current dataset'
)


# ============================================================================
# GLOBAL STATE
# ============================================================================

# Store the latest drift check result
latest_result: Optional[DriftCheckResult] = None
scheduler: Optional[BackgroundScheduler] = None


# ============================================================================
# DRIFT CHECK LOGIC
# ============================================================================

def run_scheduled_drift_check():
    """
    Run a drift check and update metrics.
    
    Called periodically by the scheduler.
    """
    global latest_result
    
    log.info("=" * 60)
    log.info("RUNNING SCHEDULED DRIFT CHECK")
    log.info("=" * 60)
    
    DRIFT_CHECKS_RUN.inc()
    start_time = datetime.now()
    
    try:
        # Load data
        reference_df, current_df = load_reference_and_current()
        
        if reference_df is None or current_df is None:
            log.warning("Not enough data for drift check")
            DRIFT_CHECK_ERRORS.inc()
            return
        
        # Update data size metrics
        DATA_REFERENCE_ROWS.set(len(reference_df))
        DATA_CURRENT_ROWS.set(len(current_df))
        
        # Run drift check
        report_path = f"/tmp/drift_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        result = run_drift_check(
            reference_df,
            current_df,
            generate_report=True,
            report_path=report_path,
        )
        
        # Store result
        latest_result = result
        
        # Update Prometheus metrics
        update_metrics(result)
        
        # Log summary
        duration = (datetime.now() - start_time).total_seconds()
        DRIFT_CHECK_DURATION.observe(duration)
        LAST_CHECK_TIME.set(datetime.now().timestamp())
        
        if result.dataset_drift_detected:
            log.warning(
                f"⚠️ DRIFT DETECTED! "
                f"{result.num_features_drifted}/{result.num_features_checked} features drifted. "
                f"Share: {result.dataset_drift_share:.2%}"
            )
        else:
            log.info(
                f"✅ No drift detected. "
                f"Checked {result.num_features_checked} features in {duration:.2f}s"
            )
        
    except Exception as e:
        log.exception(f"Drift check failed: {e}")
        DRIFT_CHECK_ERRORS.inc()


def update_metrics(result: DriftCheckResult):
    """Update Prometheus metrics from drift check result."""
    
    # Overall drift status
    DRIFT_DETECTED.set(1 if result.dataset_drift_detected else 0)
    DRIFT_SHARE.set(result.dataset_drift_share)
    DRIFT_FEATURES_TOTAL.set(result.num_features_checked)
    DRIFT_FEATURES_DRIFTED.set(result.num_features_drifted)
    
    # Per-feature metrics
    for feature_result in result.feature_results:
        DRIFT_SCORE.labels(feature=feature_result.feature_name).set(
            feature_result.drift_score
        )
        FEATURE_DRIFT_DETECTED.labels(feature=feature_result.feature_name).set(
            1 if feature_result.drift_detected else 0
        )
        
        if feature_result.reference_mean is not None:
            FEATURE_MEAN.labels(
                feature=feature_result.feature_name,
                dataset='reference'
            ).set(feature_result.reference_mean)
        
        if feature_result.current_mean is not None:
            FEATURE_MEAN.labels(
                feature=feature_result.feature_name,
                dataset='current'
            ).set(feature_result.current_mean)


# ============================================================================
# FASTAPI APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    global scheduler
    
    log.info("=" * 60)
    log.info("STARTING DRIFT DETECTOR SERVICE")
    log.info("=" * 60)
    log.info(f"Check interval: {config.drift.check_interval_minutes} minutes")
    log.info(f"Reference window: {config.drift.reference_window_size} rows")
    log.info(f"Current window: {config.drift.current_window_size} rows")
    log.info(f"Monitored features: {config.drift.monitored_features}")
    
    # Start Prometheus metrics server
    log.info(f"Starting metrics server on port {config.server.metrics_port}")
    start_http_server(config.server.metrics_port)
    
    # Start scheduler for periodic drift checks
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scheduled_drift_check,
        'interval',
        minutes=config.drift.check_interval_minutes,
        id='drift_check',
        name='Periodic Drift Check',
        next_run_time=datetime.now(),  # Run immediately on startup
    )
    scheduler.start()
    log.info(f"Scheduler started. First check in a few seconds...")
    
    yield
    
    # Shutdown
    log.info("Shutting down drift detector...")
    if scheduler:
        scheduler.shutdown()


app = FastAPI(
    title="Fraud Detection Drift Detector",
    description="Monitors feature and prediction drift in the fraud detection system",
    version="1.0.0",
    lifespan=lifespan,
)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "drift-detector",
        "last_check": latest_result.timestamp.isoformat() if latest_result else None,
        "drift_detected": latest_result.dataset_drift_detected if latest_result else None,
    }


@app.get("/drift/status")
async def get_drift_status():
    """
    Get current drift status.
    
    Returns the results of the most recent drift check.
    """
    if latest_result is None:
        return {
            "status": "no_data",
            "message": "No drift check has been run yet. Wait for scheduled check or call /drift/run"
        }
    
    return {
        "status": "drift_detected" if latest_result.dataset_drift_detected else "no_drift",
        "result": latest_result.to_dict(),
    }


@app.get("/drift/run")
async def run_manual_drift_check():
    """
    Trigger a manual drift check.
    
    Use this for immediate checks instead of waiting for scheduled check.
    """
    log.info("Manual drift check triggered via API")
    
    # Run in current thread (blocking)
    run_scheduled_drift_check()
    
    if latest_result is None:
        raise HTTPException(status_code=500, detail="Drift check failed")
    
    return {
        "status": "completed",
        "result": latest_result.to_dict(),
    }


@app.get("/drift/report")
async def get_drift_report():
    """
    Download the latest HTML drift report.
    
    Returns a detailed Evidently report with visualizations.
    """
    if latest_result is None or latest_result.html_report_path is None:
        raise HTTPException(
            status_code=404,
            detail="No drift report available. Run a drift check first."
        )
    
    if not os.path.exists(latest_result.html_report_path):
        raise HTTPException(
            status_code=404,
            detail="Report file not found"
        )
    
    return FileResponse(
        latest_result.html_report_path,
        media_type="text/html",
        filename="drift_report.html"
    )


@app.get("/drift/stats")
async def get_stats():
    """
    Get statistics about available data.
    
    Shows how much data is available for drift detection.
    """
    stats = get_data_stats()
    return {
        "data_stats": stats,
        "config": {
            "check_interval_minutes": config.drift.check_interval_minutes,
            "reference_window_size": config.drift.reference_window_size,
            "current_window_size": config.drift.current_window_size,
            "drift_threshold": config.drift.drift_threshold,
            "monitored_features": config.drift.monitored_features,
        }
    }


@app.get("/drift/simple")
async def get_simple_drift():
    """
    Get simple drift metrics (without Evidently).
    
    Useful for quick checks or debugging.
    """
    reference_df, current_df = load_reference_and_current()
    
    if reference_df is None or current_df is None:
        return {"status": "insufficient_data"}
    
    metrics = calculate_simple_drift_metrics(reference_df, current_df)
    return {
        "status": "ok",
        "reference_rows": len(reference_df),
        "current_rows": len(current_df),
        "features": metrics,
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "Fraud Detection Drift Detector",
        "version": "1.0.0",
        "status": "drift_detected" if (latest_result and latest_result.dataset_drift_detected) else "no_drift",
        "endpoints": {
            "GET /health": "Health check",
            "GET /drift/status": "Current drift status",
            "GET /drift/run": "Trigger manual drift check",
            "GET /drift/report": "Download HTML report",
            "GET /drift/stats": "Data statistics",
            "GET /drift/simple": "Simple drift metrics",
            "GET /metrics": "Prometheus metrics",
        }
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.server.api_port,
    )
