INSERT INTO dm.dim_sale_date (
    sale_date_sk,
    full_date,
    day_of_month,
    month_num,
    month_name,
    quarter_num,
    year_num,
    day_of_week_num,
    day_of_week_name
)
SELECT DISTINCT
    v.sale_date_sk,
    v.sale_date,
    EXTRACT(DAY FROM v.sale_date)::SMALLINT,
    EXTRACT(MONTH FROM v.sale_date)::SMALLINT,
    to_char(v.sale_date, 'FMMonth'),
    EXTRACT(QUARTER FROM v.sale_date)::SMALLINT,
    EXTRACT(YEAR FROM v.sale_date)::SMALLINT,
    EXTRACT(ISODOW FROM v.sale_date)::SMALLINT,
    to_char(v.sale_date, 'FMDay')
FROM raw.v_mock_data_clean v
WHERE v.sale_date IS NOT NULL
ON CONFLICT (sale_date_sk) DO NOTHING;

INSERT INTO dm.dim_customer (
    customer_email,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_country,
    customer_postal_code
)
SELECT DISTINCT
    v.customer_email,
    v.customer_first_name,
    v.customer_last_name,
    v.customer_age,
    v.customer_country,
    v.customer_postal_code
FROM raw.v_mock_data_clean v
ON CONFLICT (customer_email) DO NOTHING;

INSERT INTO dm.dim_customer_pet (
    customer_pet_bk,
    customer_sk,
    pet_type,
    pet_name,
    pet_breed,
    pet_category
)
SELECT DISTINCT
    v.customer_pet_bk,
    c.customer_sk,
    v.customer_pet_type,
    v.customer_pet_name,
    v.customer_pet_breed,
    v.pet_category
FROM raw.v_mock_data_clean v
JOIN dm.dim_customer c
  ON c.customer_email = v.customer_email
ON CONFLICT (customer_pet_bk) DO NOTHING;

INSERT INTO dm.dim_seller (
    seller_email,
    seller_first_name,
    seller_last_name,
    seller_country,
    seller_postal_code
)
SELECT DISTINCT
    v.seller_email,
    v.seller_first_name,
    v.seller_last_name,
    v.seller_country,
    v.seller_postal_code
FROM raw.v_mock_data_clean v
ON CONFLICT (seller_email) DO NOTHING;

INSERT INTO dm.dim_supplier (
    supplier_email,
    supplier_name,
    supplier_contact,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
SELECT DISTINCT
    v.supplier_email,
    v.supplier_name,
    v.supplier_contact,
    v.supplier_phone,
    v.supplier_address,
    v.supplier_city,
    v.supplier_country
FROM raw.v_mock_data_clean v
ON CONFLICT (supplier_email) DO NOTHING;

INSERT INTO dm.dim_store_location (
    store_location_bk,
    store_location,
    store_city,
    store_state,
    store_country
)
SELECT DISTINCT
    v.store_location_bk,
    v.store_location,
    v.store_city,
    v.store_state,
    v.store_country
FROM raw.v_mock_data_clean v
ON CONFLICT (store_location_bk) DO NOTHING;

INSERT INTO dm.dim_store (
    store_email,
    store_location_sk,
    store_name,
    store_phone
)
SELECT DISTINCT
    v.store_email,
    l.store_location_sk,
    v.store_name,
    v.store_phone
FROM raw.v_mock_data_clean v
JOIN dm.dim_store_location l
  ON l.store_location_bk = v.store_location_bk
ON CONFLICT (store_email) DO NOTHING;

INSERT INTO dm.dim_product_category (
    product_category_bk,
    product_category,
    pet_category
)
SELECT DISTINCT
    v.product_category_bk,
    v.product_category,
    v.pet_category
FROM raw.v_mock_data_clean v
ON CONFLICT (product_category_bk) DO NOTHING;

INSERT INTO dm.dim_product (
    product_bk,
    product_category_sk,
    supplier_sk,
    product_name,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
SELECT DISTINCT
    v.product_bk,
    pc.product_category_sk,
    s.supplier_sk,
    v.product_name,
    v.product_price,
    v.product_weight,
    v.product_color,
    v.product_size,
    v.product_brand,
    v.product_material,
    v.product_description,
    v.product_rating,
    v.product_reviews,
    v.product_release_date,
    v.product_expiry_date
FROM raw.v_mock_data_clean v
JOIN dm.dim_product_category pc
  ON pc.product_category_bk = v.product_category_bk
JOIN dm.dim_supplier s
  ON s.supplier_email = v.supplier_email
ON CONFLICT (product_bk) DO NOTHING;

INSERT INTO dm.fact_sales (
    source_raw_id,
    source_row_id,
    sale_date_sk,
    customer_pet_sk,
    seller_sk,
    product_sk,
    store_sk,
    sale_quantity,
    sale_total_price,
    source_sale_customer_id,
    source_sale_seller_id,
    source_sale_product_id,
    source_product_quantity
)
SELECT
    v.raw_id,
    v.source_row_id,
    v.sale_date_sk,
    cp.customer_pet_sk,
    s.seller_sk,
    p.product_sk,
    st.store_sk,
    v.sale_quantity,
    v.sale_total_price,
    v.sale_customer_id,
    v.sale_seller_id,
    v.sale_product_id,
    v.product_quantity
FROM raw.v_mock_data_clean v
JOIN dm.dim_customer_pet cp
  ON cp.customer_pet_bk = v.customer_pet_bk
JOIN dm.dim_seller s
  ON s.seller_email = v.seller_email
JOIN dm.dim_product p
  ON p.product_bk = v.product_bk
JOIN dm.dim_store st
  ON st.store_email = v.store_email
WHERE v.sale_date IS NOT NULL
ON CONFLICT (source_raw_id) DO NOTHING;

ANALYZE raw.mock_data;
ANALYZE dm.dim_customer;
ANALYZE dm.dim_customer_pet;
ANALYZE dm.dim_seller;
ANALYZE dm.dim_supplier;
ANALYZE dm.dim_store_location;
ANALYZE dm.dim_store;
ANALYZE dm.dim_product_category;
ANALYZE dm.dim_product;
ANALYZE dm.dim_sale_date;
ANALYZE dm.fact_sales;

DO $$
DECLARE
    v_raw_cnt BIGINT;
    v_fact_cnt BIGINT;
BEGIN
    SELECT COUNT(*) INTO v_raw_cnt FROM raw.mock_data;
    SELECT COUNT(*) INTO v_fact_cnt FROM dm.fact_sales;

    IF v_raw_cnt <> v_fact_cnt THEN
        RAISE EXCEPTION 'Row count mismatch: raw.mock_data=% and dm.fact_sales=%', v_raw_cnt, v_fact_cnt;
    END IF;
END $$;