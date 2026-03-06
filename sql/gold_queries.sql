-- sql/gold_queries.sql
-- Analytics queries for dashboard

-- Daily revenue trend (last 30 days)
SELECT report_date, total_revenue, net_revenue
FROM daily_revenue
WHERE report_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY report_date;

-- Top payment methods by volume
SELECT payment_method, transaction_count, total_amount
FROM payment_method_summary
ORDER BY total_amount DESC;

-- Transaction status breakdown
SELECT transaction_status, COUNT(*) as count, SUM(amount) as total
FROM transactions_gold
GROUP BY transaction_status;
