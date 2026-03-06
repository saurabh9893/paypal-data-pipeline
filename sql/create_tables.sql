-- sql/create_tables.sql
-- Azure SQL schema for Gold layer

CREATE TABLE transactions_gold (
    transaction_id      VARCHAR(50)    PRIMARY KEY,
    transaction_date    DATE           NOT NULL,
    amount              DECIMAL(18,2)  NOT NULL,
    currency_code       VARCHAR(10),
    transaction_status  VARCHAR(50),
    payment_method      VARCHAR(50),
    payer_email         VARCHAR(255),
    merchant_name       VARCHAR(255),
    created_at          DATETIME       DEFAULT GETDATE()
);

CREATE TABLE daily_revenue (
    report_date         DATE           PRIMARY KEY,
    total_transactions  INT,
    total_revenue       DECIMAL(18,2),
    total_refunds       DECIMAL(18,2),
    net_revenue         DECIMAL(18,2),
    created_at          DATETIME       DEFAULT GETDATE()
);

CREATE TABLE payment_method_summary (
    payment_method      VARCHAR(50)    PRIMARY KEY,
    transaction_count   INT,
    total_amount        DECIMAL(18,2),
    created_at          DATETIME       DEFAULT GETDATE()
);
