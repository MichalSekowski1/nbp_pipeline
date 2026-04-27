-- ============================================================================
-- GOLD LAYER: NBP Exchange Rate Analytics
-- ============================================================================
-- This script creates business-ready analytical tables from the Silver layer
-- Answers key questions about currency depreciation and appreciation patterns
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. HISTORICAL EXTREMES: Highest and Lowest Exchange Rates per Currency
-- ----------------------------------------------------------------------------
-- Answers: When did each currency have its highest/lowest value against PLN?

CREATE OR REPLACE TABLE workspace.gold.nbp_historical_extremes AS
WITH ranked_rates AS (
  SELECT 
    currency_code,
    effectiveDate,
    mid,
    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY mid ASC) AS rank_lowest,
    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY mid DESC) AS rank_highest
  FROM workspace.silver.nbp_exchange_rates_silver
),
lowest_rates AS (
  SELECT 
    currency_code,
    effectiveDate AS date_lowest_rate,
    mid AS lowest_rate,
    'Highest Depreciation (Lowest Value)' AS interpretation
  FROM ranked_rates
  WHERE rank_lowest = 1
),
highest_rates AS (
  SELECT 
    currency_code,
    effectiveDate AS date_highest_rate,
    mid AS highest_rate,
    'Highest Appreciation (Highest Value)' AS interpretation
  FROM ranked_rates
  WHERE rank_highest = 1
)
SELECT 
  l.currency_code,
  -- Lowest historical rate (highest depreciation)
  l.lowest_rate,
  l.date_lowest_rate,
  -- Highest historical rate (highest appreciation)
  h.highest_rate,
  h.date_highest_rate,
  -- Calculate total range
  h.highest_rate - l.lowest_rate AS historical_range,
  ROUND(((h.highest_rate - l.lowest_rate) / l.lowest_rate * 100), 2) AS range_percentage,
  CURRENT_TIMESTAMP() AS calculated_at
FROM lowest_rates l
INNER JOIN highest_rates h 
  ON l.currency_code = h.currency_code;

-- Display results
SELECT * FROM workspace.gold.nbp_historical_extremes ORDER BY currency_code;


-- ----------------------------------------------------------------------------
-- 2. DAILY CHANGES: Day-over-Day Exchange Rate Movements
-- ----------------------------------------------------------------------------
-- Calculate daily changes to identify extreme movements

CREATE OR REPLACE TABLE workspace.gold.nbp_daily_changes AS
SELECT 
  currency_code,
  effectiveDate,
  mid AS current_rate,
  LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate) AS previous_rate,
  mid - LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate) AS daily_change_absolute,
  ROUND(
    ((mid - LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate)) 
    / LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate) * 100), 
    4
  ) AS daily_change_percentage,
  CASE 
    WHEN mid > LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate) THEN 'Appreciation'
    WHEN mid < LAG(mid) OVER (PARTITION BY currency_code ORDER BY effectiveDate) THEN 'Depreciation'
    ELSE 'No Change'
  END AS movement_direction,
  CURRENT_TIMESTAMP() AS calculated_at
FROM workspace.silver.nbp_exchange_rates_silver;


-- ----------------------------------------------------------------------------
-- 3. EXTREME DAILY MOVEMENTS: Biggest Single-Day Changes per Currency
-- ----------------------------------------------------------------------------
-- Answers: When did each currency experience its most extreme daily movements?

CREATE OR REPLACE TABLE workspace.gold.nbp_extreme_daily_movements AS
WITH daily_changes_filtered AS (
  SELECT 
    currency_code,
    effectiveDate,
    current_rate,
    previous_rate,
    daily_change_absolute,
    daily_change_percentage,
    movement_direction
  FROM workspace.gold.nbp_daily_changes
  WHERE previous_rate IS NOT NULL  -- Exclude first record with no prior day
),
extreme_depreciation AS (
  SELECT 
    currency_code,
    effectiveDate AS date_biggest_depreciation,
    current_rate AS rate_on_depreciation_date,
    previous_rate AS previous_rate_depreciation,
    daily_change_absolute AS biggest_daily_fall_absolute,
    daily_change_percentage AS biggest_daily_fall_percentage,
    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY daily_change_absolute ASC) AS rank_dep
  FROM daily_changes_filtered
  WHERE movement_direction = 'Depreciation'
),
extreme_appreciation AS (
  SELECT 
    currency_code,
    effectiveDate AS date_biggest_appreciation,
    current_rate AS rate_on_appreciation_date,
    previous_rate AS previous_rate_appreciation,
    daily_change_absolute AS biggest_daily_rise_absolute,
    daily_change_percentage AS biggest_daily_rise_percentage,
    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY daily_change_absolute DESC) AS rank_app
  FROM daily_changes_filtered
  WHERE movement_direction = 'Appreciation'
)
SELECT 
  dep.currency_code,
  -- Biggest depreciation (fall in value)
  dep.date_biggest_depreciation,
  dep.rate_on_depreciation_date,
  dep.previous_rate_depreciation,
  dep.biggest_daily_fall_absolute,
  dep.biggest_daily_fall_percentage,
  -- Biggest appreciation (rise in value)
  app.date_biggest_appreciation,
  app.rate_on_appreciation_date,
  app.previous_rate_appreciation,
  app.biggest_daily_rise_absolute,
  app.biggest_daily_rise_percentage,
  CURRENT_TIMESTAMP() AS calculated_at
FROM extreme_depreciation dep
INNER JOIN extreme_appreciation app 
  ON dep.currency_code = app.currency_code
WHERE dep.rank_dep = 1 AND app.rank_app = 1;

-- Display results
SELECT * FROM workspace.gold.nbp_extreme_daily_movements ORDER BY currency_code;


-- ----------------------------------------------------------------------------
-- 4. EXECUTIVE SUMMARY: Combined View for Business Users
-- ----------------------------------------------------------------------------
-- Single comprehensive view combining all key insights

CREATE OR REPLACE VIEW workspace.gold.nbp_executive_summary AS
SELECT 
  h.currency_code,
  -- Historical extremes
  h.lowest_rate AS all_time_low_rate,
  h.date_lowest_rate AS all_time_low_date,
  h.highest_rate AS all_time_high_rate,
  h.date_highest_rate AS all_time_high_date,
  h.historical_range AS volatility_range,
  h.range_percentage AS volatility_percentage,
  -- Extreme daily movements
  e.biggest_daily_fall_absolute,
  e.biggest_daily_fall_percentage,
  e.date_biggest_depreciation AS worst_single_day_date,
  e.biggest_daily_rise_absolute,
  e.biggest_daily_rise_percentage,
  e.date_biggest_appreciation AS best_single_day_date,
  -- Metadata
  h.calculated_at AS last_updated
FROM workspace.gold.nbp_historical_extremes h
INNER JOIN workspace.gold.nbp_extreme_daily_movements e 
  ON h.currency_code = e.currency_code;

-- Display executive summary
SELECT * FROM workspace.gold.nbp_executive_summary ORDER BY currency_code;


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Quick check: Record counts per gold table
SELECT 'Historical Extremes' AS table_name, COUNT(*) AS record_count 
FROM workspace.gold.nbp_historical_extremes
UNION ALL
SELECT 'Daily Changes', COUNT(*) 
FROM workspace.gold.nbp_daily_changes
UNION ALL
SELECT 'Extreme Daily Movements', COUNT(*) 
FROM workspace.gold.nbp_extreme_daily_movements;
