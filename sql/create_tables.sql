-- ============================================================
--  СКРИПТ СОЗДАНИЯ ТАБЛИЦ 
-- ============================================================

-- 1. STAGING: сырые артикулы
CREATE TABLE stg_articles_raw (
    article VARCHAR(100),
    internal_code VARCHAR(100),
    product_name VARCHAR(500),
    source VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- 2. СПРАВОЧНИК МАППИНГА
CREATE TABLE dim_article_mapping (
    internal_code VARCHAR(100) PRIMARY KEY,
    wb_article VARCHAR(100),
    ozon_article VARCHAR(100),
    product_name VARCHAR(500),
    match_method VARCHAR(50),
    onec_article VARCHAR(100),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_mapping_wb ON dim_article_mapping(wb_article);
CREATE INDEX idx_mapping_ozon ON dim_article_mapping(ozon_article);

-- 3. STAGING: заказы WB (старый метод, оперативные)
CREATE TABLE stg_wb_orders (
    order_id VARCHAR(50),
    "nmId" VARCHAR(50),
    "supplierArticle" VARCHAR(100),
    barcode VARCHAR(100),
    date DATE,
    "lastChangeDate" TIMESTAMP,
    "warehouseName" VARCHAR(100),
    "countryName" VARCHAR(50),
    "oblastOkrugName" VARCHAR(100),
    "regionName" VARCHAR(100),
    "totalPrice" DECIMAL(10,2),
    "discountPercent" INT,
    spp DECIMAL(10,2),
    "finishedPrice" DECIMAL(10,2),
    "priceWithDisc" DECIMAL(10,2),
    "isCancel" BOOLEAN,
    "cancelDate" DATE,
    "orderType" VARCHAR(50),
    sticker VARCHAR(200),
    "gNumber" VARCHAR(50),
    srid VARCHAR(100),
    "warehouseType" VARCHAR(50),
    "incomeID" INT,
    "isSupply" BOOLEAN,
    "isRealization" BOOLEAN,
    category VARCHAR(200),
    subject VARCHAR(200),
    brand VARCHAR(100),
    "techSize" VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT stg_wb_orders_unique UNIQUE ("gNumber", "nmId")
);
CREATE INDEX idx_stg_wb_orders_date ON stg_wb_orders(date);

-- 4. STAGING: финансовый отчёт WB (полная история)
CREATE TABLE stg_wb_finance (
    realizationreport_id BIGINT,
    date DATE,
    suppliercontract_code VARCHAR(50),
    rid BIGINT,
    rr_dt DATE,
    rrd_id BIGINT,
    gi_id BIGINT,
    subject_name VARCHAR(200),
    nm_id BIGINT,
    brand_name VARCHAR(100),
    sa_name VARCHAR(100),
    ts_name VARCHAR(50),
    barcode VARCHAR(50),
    doc_type_name VARCHAR(50),
    quantity INT,
    retail_price DECIMAL(10,2),
    retail_amount DECIMAL(10,2),
    sale_percent DECIMAL(5,2),
    commission_percent DECIMAL(5,2),
    ppvz_for_pay DECIMAL(10,2),
    ppvz_reward DECIMAL(10,2),
    ppvz_vw DECIMAL(10,2),
    ppvz_vw_nds DECIMAL(10,2),
    ppvz_office_id BIGINT,
    ppvz_office_name VARCHAR(200),
    ppvz_supplier_id BIGINT,
    ppvz_supplier_name VARCHAR(200),
    ppvz_kvw_prc DECIMAL(5,2),
    shk_id BIGINT,
    retail_price_withdisc_rub DECIMAL(10,2),
    delivery_amount DECIMAL(10,2),
    return_amount DECIMAL(10,2),
    delivery_rub DECIMAL(10,2),
    gi_box_type_name VARCHAR(100),
    product_discount_for_report DECIMAL(10,2),
    supplier_promo VARCHAR(100),
    ppvz_spp_prc DECIMAL(5,2),
    ppvz_kvw_prc_base DECIMAL(5,2),
    ppvz_sales_commission DECIMAL(10,2),
    ppvz_for_pay_nds DECIMAL(10,2),
    ppvz_reward_nds DECIMAL(10,2),
    acquiring_fee DECIMAL(10,2),
    acquiring_bank VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_wb_finance_date ON stg_wb_finance(date);
CREATE INDEX idx_wb_finance_nmid ON stg_wb_finance(nm_id);

-- 5. STAGING: отправления Ozon FBO
CREATE TABLE stg_ozon_orders (
    posting_number VARCHAR(50) PRIMARY KEY,
    order_id BIGINT,
    order_number VARCHAR(50),
    posting_status VARCHAR(50),
    status VARCHAR(50),
    offer_id VARCHAR(100),
    sku BIGINT,
    name VARCHAR(255),
    quantity INT,
    price DECIMAL(10,2),
    delivery_schema VARCHAR(50),
    created_at TIMESTAMP,
    in_process_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_stg_ozon_orders_created ON stg_ozon_orders(created_at);

-- 6. 1С: остатки и себестоимость
CREATE TABLE onec_stocks (
    internal_code VARCHAR(100) PRIMARY KEY,
    product_name VARCHAR(500),
    current_stock INT,
    reserved_stock INT,
    available_stock INT GENERATED ALWAYS AS (current_stock - reserved_stock) STORED,
    cost_price DECIMAL(10,2),
    warehouse VARCHAR(100),
    stock_date DATE,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 7. CRM: клиенты
CREATE TABLE crm_customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    registration_date DATE,
    city VARCHAR(100),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 8. CRM: заказы
CREATE TABLE crm_orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES crm_customers(customer_id),
    order_date DATE,
    product_name VARCHAR(500),
    quantity INT,
    price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_crm_orders_date ON crm_orders(order_date);

-- 9. Google Analytics
CREATE TABLE ga_sessions (
    date DATE PRIMARY KEY,
    sessions INT,
    users INT,
    bounce_rate DECIMAL(5,3)
);

-- 10. ВИТРИНА ДАННЫХ
CREATE TABLE IF NOT EXISTS mart_unified_sales (
    internal_code VARCHAR(100),
	product_name VARCHAR(500),
    wb_article VARCHAR(100),
    ozon_article VARCHAR(100),
    onec_article VARCHAR(100),
    wb_sales_count INT DEFAULT 0,
    ozon_sales_count INT DEFAULT 0,
    crm_orders_count INT DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0,
    total_profit DECIMAL(12,2),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_mart_unified_updated ON mart_unified_sales(updated_at);

-- Уведомление
DO $$ BEGIN RAISE NOTICE '✅ Все таблицы созданы успешно!'; END $$;