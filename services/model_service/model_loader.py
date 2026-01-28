"""
MODEL LOADER - Load trained model from MLflow
================================================================================

This module handles loading the trained model from MLflow registry.
The model is loaded ONCE at startup for fast inference.

================================================================================
"""

import logging
from typing import Optional, Tuple, Any

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd

from config import config

log = logging.getLogger("model_service.model_loader")


class ModelLoader:
    """
    Loads and manages the fraud detection model.
    
    Usage:
        loader = ModelLoader()
        loader.load()
        
        # Score a transaction
        score = loader.predict(features_df)
    """
    
    def __init__(self):
        self.model = None
        self.model_name: Optional[str] = None
        self.model_version: Optional[str] = None
        self.feature_names: Optional[list] = None
        
    def load(self) -> bool:
        """
        Load model from MLflow.
        
        Returns:
            True if model loaded successfully, False otherwise
        """
        try:
            mlflow.set_tracking_uri(config.mlflow.tracking_uri)
            
            model_uri = f"models:/{config.mlflow.model_name}/{config.mlflow.model_version}"
            log.info(f"Loading model from: {model_uri}")
            
            self.model = mlflow.sklearn.load_model(model_uri)
            self.model_name = config.mlflow.model_name
            self.model_version = config.mlflow.model_version
            
            # Get feature names if available
            if hasattr(self.model, 'feature_names_in_'):
                self.feature_names = list(self.model.feature_names_in_)
            elif hasattr(self.model, 'get_booster'):
                # XGBoost
                self.feature_names = self.model.get_booster().feature_names
            
            log.info(f"Model loaded successfully: {self.model_name} v{self.model_version}")
            if self.feature_names:
                log.info(f"Model expects {len(self.feature_names)} features: {self.feature_names}")
            
            return True
            
        except Exception as e:
            log.exception(f"Failed to load model: {e}")
            return False
    
    def is_loaded(self) -> bool:
        """Check if model is loaded."""
        return self.model is not None
    
    def predict_proba(self, features: pd.DataFrame) -> np.ndarray:
        """
        Get fraud probability for transactions.
        
        Args:
            features: DataFrame with features (one row per transaction)
        
        Returns:
            Array of fraud probabilities (0-1)
        """
        if not self.is_loaded():
            raise RuntimeError("Model not loaded. Call load() first.")
        
        # Ensure feature order matches what model expects
        if self.feature_names:
            # Only use features the model knows about
            available = [f for f in self.feature_names if f in features.columns]
            missing = [f for f in self.feature_names if f not in features.columns]
            
            if missing:
                log.warning(f"Missing features (will use 0): {missing}")
                for f in missing:
                    features[f] = 0
            
            features = features[self.feature_names]
        
        # Get probability of class 1 (fraud)
        proba = self.model.predict_proba(features)
        return proba[:, 1]
    
    def get_feature_importances(self) -> dict:
        """
        Get feature importances from the model.
        
        Returns:
            Dict mapping feature name to importance score
        """
        if not self.is_loaded():
            return {}
        
        if hasattr(self.model, 'feature_importances_'):
            importances = self.model.feature_importances_
            if self.feature_names and len(self.feature_names) == len(importances):
                return dict(zip(self.feature_names, importances))
        
        return {}


# Global model instance (loaded once at startup)
model_loader = ModelLoader()
