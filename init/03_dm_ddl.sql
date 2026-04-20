CREATE SCHEMA IF NOT EXISTS dm;

DROP TABLE IF EXISTS dm.fact_sales CASCADE;
DROP TABLE IF EXISTS dm.dim_sale_date CASCADE;
DROP TABLE IF EXISTS dm.dim_product CASCADE;
DROP TABLE IF EXISTS dm.dim_product_category CASCADE;
DROP TABLE IF EXISTS dm.dim_supplier CASCADE;
DROP TABLE IF EXISTS dm.dim_store CASCADE;
DROP TABLE IF EXISTS dm.dim_store_location CASCADE;
DROP TABLE IF EXISTS dm.dim_seller CASCADE;
DROP TABLE IF EXISTS dm.dim_customer_pet CASCADE;
DROP TABLE IF EXISTS dm.dim_customer CASCADE;
DROP VIEW IF EXISTS raw.v_mock_data_clean;

CREATE VIEW raw.v_mock_data_clean AS
SELECT
    raw_id,
    NULLIF(trim(source_row_id), '')::INTEGER AS source_row_id,

    COALESCE(NULLIF(trim(customer_first_name), ''), 'Unknown') AS customer_first_name,
    COALESCE(NULLIF(trim(customer_last_name), ''), 'Unknown') AS customer_last_name,
    NULLIF(trim(customer_age), '')::INTEGER AS customer_age,
    lower(COALESCE(NULLIF(trim(customer_email), ''), 'unknown@example.com')) AS customer_email,
    COALESCE(NULLIF(trim(customer_country), ''), 'Unknown') AS customer_country,
    NULLIF(trim(customer_postal_code), '') AS customer_postal_code,

    COALESCE(NULLIF(trim(customer_pet_type), ''), 'Unknown') AS customer_pet_type,
    COALESCE(NULLIF(trim(customer_pet_name), ''), 'Unknown') AS customer_pet_name,
    COALESCE(NULLIF(trim(customer_pet_breed), ''), 'Unknown') AS customer_pet_breed,
    COALESCE(NULLIF(trim(pet_category), ''), 'Unknown') AS pet_category,

    COALESCE(NULLIF(trim(seller_first_name), ''), 'Unknown') AS seller_first_name,
    COALESCE(NULLIF(trim(seller_last_name), ''), 'Unknown') AS seller_last_name,
    lower(COALESCE(NULLIF(trim(seller_email), ''), 'unknown_seller@example.com')) AS seller_email,
    COALESCE(NULLIF(trim(seller_country), ''), 'Unknown') AS seller_country,
    NULLIF(trim(seller_postal_code), '') AS seller_postal_code,

    COALESCE(NULLIF(trim(product_name), ''), 'Unknown product') AS product_name,
    COALESCE(NULLIF(trim(product_category), ''), 'Unknown category') AS product_category,
    NULLIF(trim(product_price), '')::NUMERIC(12,2) AS product_price,
    NULLIF(trim(product_quantity), '')::INTEGER AS product_quantity,
    NULLIF(trim(product_weight), '')::NUMERIC(10,2) AS product_weight,
    COALESCE(NULLIF(trim(product_color), ''), 'Unknown') AS product_color,
    COALESCE(NULLIF(trim(product_size), ''), 'Unknown') AS product_size,
    COALESCE(NULLIF(trim(product_brand), ''), 'Unknown') AS product_brand,
    COALESCE(NULLIF(trim(product_material), ''), 'Unknown') AS product_material,
    COALESCE(NULLIF(trim(product_description), ''), '') AS product_description,
    NULLIF(trim(product_rating), '')::NUMERIC(3,1) AS product_rating,
    NULLIF(trim(product_reviews), '')::INTEGER AS product_reviews,
    CASE
        WHEN NULLIF(trim(product_release_date), '') IS NULL THEN NULL
        ELSE to_date(trim(product_release_date), 'MM/DD/YYYY')
    END AS product_release_date,
    CASE
        WHEN NULLIF(trim(product_expiry_date), '') IS NULL THEN NULL
        ELSE to_date(trim(product_expiry_date), 'MM/DD/YYYY')
    END AS product_expiry_date,

    CASE
        WHEN NULLIF(trim(sale_date), '') IS NULL THEN NULL
        ELSE to_date(trim(sale_date), 'MM/DD/YYYY')
    END AS sale_date,
    NULLIF(trim(sale_customer_id), '')::INTEGER AS sale_customer_id,
    NULLIF(trim(sale_seller_id), '')::INTEGER AS sale_seller_id,
    NULLIF(trim(sale_product_id), '')::INTEGER AS sale_product_id,
    COALESCE(NULLIF(trim(sale_quantity), '')::INTEGER, 0) AS sale_quantity,
    COALESCE(NULLIF(trim(sale_total_price), '')::NUMERIC(12,2), 0) AS sale_total_price,

    COALESCE(NULLIF(trim(store_name), ''), 'Unknown store') AS store_name,
    COALESCE(NULLIF(trim(store_location), ''), '') AS store_location,
    COALESCE(NULLIF(trim(store_city), ''), '') AS store_city,
    COALESCE(NULLIF(trim(store_state), ''), '') AS store_state,
    COALESCE(NULLIF(trim(store_country), ''), 'Unknown') AS store_country,
    NULLIF(trim(store_phone), '') AS store_phone,
    lower(COALESCE(NULLIF(trim(store_email), ''), 'unknown_store@example.com')) AS store_email,

    COALESCE(NULLIF(trim(supplier_name), ''), 'Unknown supplier') AS supplier_name,
    COALESCE(NULLIF(trim(supplier_contact), ''), 'Unknown contact') AS supplier_contact,
    lower(COALESCE(NULLIF(trim(supplier_email), ''), 'unknown_supplier@example.com')) AS supplier_email,
    NULLIF(trim(supplier_phone), '') AS supplier_phone,
    COALESCE(NULLIF(trim(supplier_address), ''), '') AS supplier_address,
    COALESCE(NULLIF(trim(supplier_city), ''), '') AS supplier_city,
    COALESCE(NULLIF(trim(supplier_country), ''), 'Unknown') AS supplier_country,

    CASE
        WHEN NULLIF(trim(sale_date), '') IS NULL THEN NULL
        ELSE CAST(to_char(to_date(trim(sale_date), 'MM/DD/YYYY'), 'YYYYMMDD') AS INTEGER)
    END AS sale_date_sk,
    md5(
        lower(COALESCE(NULLIF(trim(customer_email), ''), 'unknown@example.com')) || '|' ||
        COALESCE(NULLIF(trim(customer_pet_type), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(customer_pet_name), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(customer_pet_breed), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(pet_category), ''), 'Unknown')
    ) AS customer_pet_bk,
    md5(
        COALESCE(NULLIF(trim(store_location), ''), '') || '|' ||
        COALESCE(NULLIF(trim(store_city), ''), '') || '|' ||
        COALESCE(NULLIF(trim(store_state), ''), '') || '|' ||
        COALESCE(NULLIF(trim(store_country), ''), 'Unknown')
    ) AS store_location_bk,
    md5(
        COALESCE(NULLIF(trim(product_category), ''), 'Unknown category') || '|' ||
        COALESCE(NULLIF(trim(pet_category), ''), 'Unknown')
    ) AS product_category_bk,
    md5(
        COALESCE(NULLIF(trim(product_name), ''), 'Unknown product') || '|' ||
        COALESCE(NULLIF(trim(product_category), ''), 'Unknown category') || '|' ||
        COALESCE(NULLIF(trim(pet_category), ''), 'Unknown') || '|' ||
        lower(COALESCE(NULLIF(trim(supplier_email), ''), 'unknown_supplier@example.com')) || '|' ||
        COALESCE(NULLIF(trim(product_price), ''), '') || '|' ||
        COALESCE(NULLIF(trim(product_weight), ''), '') || '|' ||
        COALESCE(NULLIF(trim(product_color), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(product_size), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(product_brand), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(product_material), ''), 'Unknown') || '|' ||
        COALESCE(NULLIF(trim(product_release_date), ''), '') || '|' ||
        COALESCE(NULLIF(trim(product_expiry_date), ''), '')
    ) AS product_bk
FROM raw.mock_data;

CREATE TABLE dm.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_email TEXT NOT NULL UNIQUE,
    customer_first_name TEXT NOT NULL,
    customer_last_name TEXT NOT NULL,
    customer_age INTEGER,
    customer_country TEXT NOT NULL,
    customer_postal_code TEXT
);

CREATE TABLE dm.dim_customer_pet (
    customer_pet_sk BIGSERIAL PRIMARY KEY,
    customer_pet_bk TEXT NOT NULL UNIQUE,
    customer_sk BIGINT NOT NULL REFERENCES dm.dim_customer(customer_sk),
    pet_type TEXT NOT NULL,
    pet_name TEXT NOT NULL,
    pet_breed TEXT NOT NULL,
    pet_category TEXT NOT NULL
);

CREATE TABLE dm.dim_seller (
    seller_sk BIGSERIAL PRIMARY KEY,
    seller_email TEXT NOT NULL UNIQUE,
    seller_first_name TEXT NOT NULL,
    seller_last_name TEXT NOT NULL,
    seller_country TEXT NOT NULL,
    seller_postal_code TEXT
);

CREATE TABLE dm.dim_supplier (
    supplier_sk BIGSERIAL PRIMARY KEY,
    supplier_email TEXT NOT NULL UNIQUE,
    supplier_name TEXT NOT NULL,
    supplier_contact TEXT NOT NULL,
    supplier_phone TEXT,
    supplier_address TEXT NOT NULL,
    supplier_city TEXT NOT NULL,
    supplier_country TEXT NOT NULL
);

CREATE TABLE dm.dim_store_location (
    store_location_sk BIGSERIAL PRIMARY KEY,
    store_location_bk TEXT NOT NULL UNIQUE,
    store_location TEXT NOT NULL,
    store_city TEXT NOT NULL,
    store_state TEXT NOT NULL,
    store_country TEXT NOT NULL
);

CREATE TABLE dm.dim_store (
    store_sk BIGSERIAL PRIMARY KEY,
    store_email TEXT NOT NULL UNIQUE,
    store_location_sk BIGINT NOT NULL REFERENCES dm.dim_store_location(store_location_sk),
    store_name TEXT NOT NULL,
    store_phone TEXT
);

CREATE TABLE dm.dim_product_category (
    product_category_sk BIGSERIAL PRIMARY KEY,
    product_category_bk TEXT NOT NULL UNIQUE,
    product_category TEXT NOT NULL,
    pet_category TEXT NOT NULL
);

CREATE TABLE dm.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,
    product_bk TEXT NOT NULL UNIQUE,
    product_category_sk BIGINT NOT NULL REFERENCES dm.dim_product_category(product_category_sk),
    supplier_sk BIGINT NOT NULL REFERENCES dm.dim_supplier(supplier_sk),
    product_name TEXT NOT NULL,
    product_price NUMERIC(12,2),
    product_weight NUMERIC(10,2),
    product_color TEXT NOT NULL,
    product_size TEXT NOT NULL,
    product_brand TEXT NOT NULL,
    product_material TEXT NOT NULL,
    product_description TEXT NOT NULL,
    product_rating NUMERIC(3,1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dm.dim_sale_date (
    sale_date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month SMALLINT NOT NULL,
    month_num SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    quarter_num SMALLINT NOT NULL,
    year_num SMALLINT NOT NULL,
    day_of_week_num SMALLINT NOT NULL,
    day_of_week_name TEXT NOT NULL
);

CREATE TABLE dm.fact_sales (
    sale_sk BIGSERIAL PRIMARY KEY,
    source_raw_id BIGINT NOT NULL UNIQUE,
    source_row_id INTEGER,
    sale_date_sk INTEGER NOT NULL REFERENCES dm.dim_sale_date(sale_date_sk),
    customer_pet_sk BIGINT NOT NULL REFERENCES dm.dim_customer_pet(customer_pet_sk),
    seller_sk BIGINT NOT NULL REFERENCES dm.dim_seller(seller_sk),
    product_sk BIGINT NOT NULL REFERENCES dm.dim_product(product_sk),
    store_sk BIGINT NOT NULL REFERENCES dm.dim_store(store_sk),
    sale_quantity INTEGER NOT NULL,
    sale_total_price NUMERIC(12,2) NOT NULL,
    source_sale_customer_id INTEGER,
    source_sale_seller_id INTEGER,
    source_sale_product_id INTEGER,
    source_product_quantity INTEGER
);

CREATE INDEX idx_fact_sales_date         ON dm.fact_sales (sale_date_sk);
CREATE INDEX idx_fact_sales_customer_pet ON dm.fact_sales (customer_pet_sk);
CREATE INDEX idx_fact_sales_seller       ON dm.fact_sales (seller_sk);
CREATE INDEX idx_fact_sales_product      ON dm.fact_sales (product_sk);
CREATE INDEX idx_fact_sales_store        ON dm.fact_sales (store_sk);