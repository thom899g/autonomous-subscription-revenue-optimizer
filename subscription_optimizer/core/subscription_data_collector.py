import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class SubscriptionDataCollector:
    """Collects subscription data from various sources and transforms it into a unified format.
    
    Attributes:
        api_keys: Dictionary containing API keys for external services.
        last_fetch_time: Timestamp of the last successful data fetch.
    """
    
    def __init__(self, config_path: str):
        """Initialize with configuration file path."""
        self.api_keys = self._load_api_keys(config_path)
        self.last_fetch_time = None
    
    def _load_api_keys(self, config_path: str) -> Dict[str, str]:
        """Load API keys from configuration file.
        
        Args:
            config_path: Path to the configuration file.
            
        Returns:
            Dictionary of API keys.
            
        Raises:
            FileNotFoundError: If configuration file not found.
            KeyError: If required keys are missing.
        """
        try:
            # Simplified example; in reality, use secure loading methods
            with open(config_path, 'r') as f:
                return {
                    'salesforce': 'your_salesforce_key',
                    'stripe': 'your_stripe_key',
                    'google_analytics': 'your_ga_key'
                }
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        except KeyError:
            raise KeyError("Missing required API keys in configuration")
    
    def fetch_salesforce_data(self) -> pd.DataFrame:
        """Fetch subscription data from Salesforce.
        
        Returns:
            DataFrame containing subscription data.
            
        Raises:
            requests.exceptions.RequestException: If API call fails.
        """
        try:
            response = requests.get(
                'https://api.salesforce.com/subscriptions',
                headers={'Authorization': f'Bearer {self.api_keys["salesforce"]}'}
            )
            response.raise_for_status()
            return pd.json_normalize(response.json())
        except requests.exceptions.RequestException as e:
            logger.error(f"Salesforce API call failed: {str(e)}")
            raise
    
    def fetch_stripe_data(self) -> pd.DataFrame:
        """Fetch payment data from Stripe.
        
        Returns:
            DataFrame containing payment data.
            
        Raises:
            requests.exceptions.RequestException: If API call fails.
        """
        try:
            response = requests.get(
                'https://api.stripe.com/payments/subscriptions',
                headers={'Authorization': f'Bearer {self.api_keys["stripe"]}'}
            )
            response.raise_for_status()
            return pd.json_normalize(response.json())
        except requests.exceptions.RequestException as e:
            logger.error(f"Stripe API call failed: {str(e)}")
            raise
    
    def fetch_google_analytics_data(self, start_date: str) -> pd.DataFrame:
        """Fetch usage data from Google Analytics.
        
        Args:
            start_date: Start date for data range in YYYY-MM-DD format.
            
        Returns:
            DataFrame containing usage data.
            
        Raises:
            requests.exceptions.RequestException: If API call fails.
        """
        end_date = datetime.today().strftime('%Y-%m-%d')
        params = {
            'start-date': start_date,
            'end-date': end_date
        }
        
        try:
            response = requests.get(
                'https://api.google-analytics.com/data',
                headers={'Authorization': f'Bearer {self.api_keys["google_analytics"]}'},
                params=params
            )
            response.raise_for_status()
            return pd.json_normalize(response.json())
        except requests.exceptions.RequestException as e:
            logger.error(f"Google Analytics API call failed: {str(e)}")
            raise
    
    def collect_all_data(self) -> pd.DataFrame:
        """Collect subscription, payment, and usage data from all sources.
        
        Returns:
            Unified DataFrame containing all subscription-related data.
        """
        try:
            # Fetch data
            salesforce_data = self.fetch_salesforce_data()
            stripe_data = self.fetch_stripe_data()
            ga_data = self.fetch_google_analytics_data(
                (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
            )
            
            # Transform and merge
            subscription_df = (
                salesforce_data.merge(
                    stripe_data[['subscription_id', 'status', 'created_at']],
                    on='subscription_id',
                    how='left'
                ).merge(
                    ga_data[['subscription_id', 'session_count', 'revenue']],
                    on='subscription_id',
                    how='left'
                )
            )
            
            self.last_fetch_time = datetime.now()
            return subscription_df
        except Exception as e:
            logger.error(f"Data collection failed: {str(e)}")
            raise