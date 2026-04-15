CREATE TABLE IF NOT EXISTS risk_metrics (
    supplier_id TEXT,
    expected_date DATE,
    actual_date DATE,
    price REAL,
    demand INTEGER,
    delay_days INTEGER,
    price_volatility REAL,
    risk_score REAL
);
