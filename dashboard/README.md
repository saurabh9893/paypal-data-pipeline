# Dashboard Setup

## Option A - Power BI (Recommended)
1. Open Power BI Desktop
2. Get Data → Azure SQL Database
3. Enter your Azure SQL Server + DB name
4. Load `transactions_gold` and `daily_revenue` tables
5. Build visuals: revenue trend, payment method pie chart, status breakdown

## Option B - Metabase (Free, Open Source)
1. Run: `docker run -d -p 3000:3000 metabase/metabase`
2. Open http://localhost:3000
3. Connect to Azure SQL Database
4. Create dashboard with pre-built queries from `sql/gold_queries.sql`
