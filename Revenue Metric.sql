WITH payments AS (
    -- Monthly Total Payment
    SELECT 
        user_id,
        game_name,
        DATE_TRUNC('month', payment_date)::DATE AS payment_month,
        COALESCE(SUM(revenue_amount_usd), 0) AS monthly_total_payment
    FROM games_payments
    GROUP BY user_id, game_name, DATE_TRUNC('month', payment_date)
),
calendar AS (
	-- Create Months
    SELECT 
        u.user_id,
        u.game_name,
        u.language,
        u.has_older_device_model,
        u.age,
        generate_series(
            (SELECT MIN(DATE_TRUNC('month', payment_date)) FROM games_payments),
            (SELECT MAX(DATE_TRUNC('month', payment_date)) FROM games_payments),
            INTERVAL '1 month'
        )::DATE AS payment_month
    FROM games_paid_users AS u
),
payment_behavior AS (
	-- Merge All Fields and Payments
    SELECT 
        ca.user_id,
        ca.game_name,
        ca.language,
        ca.has_older_device_model,
        ca.age,
        ca.payment_month,
        COALESCE(pa.monthly_total_payment, 0) AS total_payment,
        
        -- calculating total_payment_previous
        COALESCE(LAG(pa.monthly_total_payment, 1) OVER (
            PARTITION BY ca.user_id, ca.game_name 
            ORDER BY ca.payment_month
        ), 0) AS total_payment_previous,
        
        -- calculating all payment before this month
        COALESCE(SUM(pa.monthly_total_payment) OVER (
            PARTITION BY ca.user_id, ca.game_name 
            ORDER BY ca.payment_month 
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS previous_total_payments
        
    FROM calendar AS ca
    LEFT JOIN payments AS pa
        ON ca.user_id = pa.user_id
        AND ca.game_name = pa.game_name
        AND ca.payment_month = pa.payment_month
),
lifecycle_stage AS (
    -- Prepare status
    SELECT 
        user_id,
        game_name,
        language,
        has_older_device_model,
        age,
        payment_month,
        total_payment,
        total_payment_previous,
        CASE 
            WHEN total_payment > 0 THEN 
                CASE 
                    WHEN total_payment_previous = 0 THEN 
                        CASE 
                            WHEN previous_total_payments = 0 THEN 'new'
                            ELSE 'back'
                        END
                    ELSE 'active'
                END
            ELSE 
                CASE 
                    WHEN total_payment_previous > 0 THEN 'churn'
                    ELSE 'inactive'
                END
        END AS user_status
    FROM payment_behavior
)
SELECT 
    user_id,
    game_name,
    language,
    has_older_device_model,
    age,
    payment_month,
    user_status,
    total_payment,
    total_payment_previous
FROM lifecycle_stage
ORDER BY payment_month, user_id, game_name;
