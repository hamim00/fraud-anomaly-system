-- ============================================================================
-- MILESTONE 2: Transaction Features Table
-- ============================================================================
-- This table stores calculated features for each transaction.
-- Features are computed by the feature_consumer service in real-time.
-- These features will be used by the ML model to detect fraud.
-- ============================================================================

CREATE TABLE IF NOT EXISTS transaction_features (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Link to original transaction
    transaction_id UUID NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    
    -- Timestamps
    event_time TIMESTAMPTZ NOT NULL,          -- When transaction happened
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- When features were calculated
    
    -- ========================================================================
    -- AMOUNT FEATURES
    -- ========================================================================
    amount NUMERIC(12,2) NOT NULL,            -- Original amount
    amount_zscore NUMERIC(8,4),               -- How unusual is this amount?
                                              -- (amount - user_avg) / user_std
                                              -- High z-score = unusual = suspicious
    
    -- ========================================================================
    -- VELOCITY FEATURES (How fast is user transacting?)
    -- ========================================================================
    user_txn_count_1h INTEGER,                -- Transactions in last 1 hour
    user_txn_count_24h INTEGER,               -- Transactions in last 24 hours
    user_txn_count_7d INTEGER,                -- Transactions in last 7 days
    
    user_amount_sum_1h NUMERIC(12,2),         -- Total spent in last 1 hour
    user_amount_sum_24h NUMERIC(12,2),        -- Total spent in last 24 hours
    
    user_avg_amount_30d NUMERIC(12,2),        -- Average transaction amount (30 days)
    user_std_amount_30d NUMERIC(12,2),        -- Std deviation of amounts (30 days)
    
    -- ========================================================================
    -- BEHAVIORAL FEATURES (Is user acting differently?)
    -- ========================================================================
    country_change_flag BOOLEAN,              -- Different country than last txn?
    device_change_flag BOOLEAN,               -- Different device than last txn?
    
    unique_countries_24h INTEGER,             -- How many countries in 24h?
    unique_merchants_24h INTEGER,             -- How many merchants in 24h?
    unique_devices_24h INTEGER,               -- How many devices in 24h?
    
    user_merchant_first_time BOOLEAN,         -- First time at this merchant?
    
    -- ========================================================================
    -- TIME FEATURES (When is user transacting?)
    -- ========================================================================
    hour_of_day INTEGER,                      -- 0-23
    day_of_week INTEGER,                      -- 0=Monday, 6=Sunday
    is_weekend BOOLEAN,                       -- Saturday or Sunday?
    is_night BOOLEAN,                         -- Between 00:00 and 06:00?
    
    minutes_since_last_txn INTEGER,           -- Minutes since last transaction
                                              -- Very small = rapid-fire = suspicious
    
    -- ========================================================================
    -- CHANNEL FEATURES
    -- ========================================================================
    channel TEXT NOT NULL,                    -- POS, ECOM, ATM
    channel_encoded INTEGER,                  -- POS=0, ECOM=1, ATM=2
    
    -- ========================================================================
    -- GEOGRAPHIC FEATURES
    -- ========================================================================
    country TEXT NOT NULL,
    is_foreign_txn BOOLEAN,                   -- Different from user's home country?
    
    -- ========================================================================
    -- LABEL (for training)
    -- ========================================================================
    label BOOLEAN,                            -- True = fraud, False = legitimate
    
    -- ========================================================================
    -- CONSTRAINTS
    -- ========================================================================
    CONSTRAINT fk_features_transaction 
        FOREIGN KEY (transaction_id) 
        REFERENCES raw_events(transaction_id)
        ON DELETE CASCADE
);

-- ============================================================================
-- INDEXES for fast queries
-- ============================================================================

-- For joining with raw_events
CREATE INDEX IF NOT EXISTS idx_features_txn_id 
    ON transaction_features(transaction_id);

-- For time-based queries (training data extraction)
CREATE INDEX IF NOT EXISTS idx_features_event_time 
    ON transaction_features(event_time DESC);

-- For user-based queries
CREATE INDEX IF NOT EXISTS idx_features_user_time 
    ON transaction_features(user_id, event_time DESC);

-- For finding labeled data (training)
CREATE INDEX IF NOT EXISTS idx_features_label 
    ON transaction_features(label) 
    WHERE label IS NOT NULL;

-- For finding fraud specifically
CREATE INDEX IF NOT EXISTS idx_features_fraud 
    ON transaction_features(event_time DESC) 
    WHERE label = true;

-- ============================================================================
-- COMMENTS (documentation)
-- ============================================================================

COMMENT ON TABLE transaction_features IS 
    'Computed features for each transaction, used for ML fraud detection';

COMMENT ON COLUMN transaction_features.amount_zscore IS 
    'Z-score of amount vs user historical average. High values indicate unusual amounts.';

COMMENT ON COLUMN transaction_features.user_txn_count_1h IS 
    'Number of transactions by this user in the past 1 hour. High values may indicate fraud.';

COMMENT ON COLUMN transaction_features.minutes_since_last_txn IS 
    'Minutes since users last transaction. Very low values may indicate rapid-fire fraud.';
