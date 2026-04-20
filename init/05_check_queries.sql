-- 10000 строк в staging
SELECT COUNT(*) AS raw_rows FROM raw.mock_data;

-- 10000 строк в таблице фактов
SELECT COUNT(*) AS fact_rows FROM dm.fact_sales;

-- Быстрая проверка заполнения измерений
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dm.dim_customer
UNION ALL
SELECT 'dim_customer_pet', COUNT(*) FROM dm.dim_customer_pet
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dm.dim_seller
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM dm.dim_supplier
UNION ALL
SELECT 'dim_store_location', COUNT(*) FROM dm.dim_store_location
UNION ALL
SELECT 'dim_store', COUNT(*) FROM dm.dim_store
UNION ALL
SELECT 'dim_product_category', COUNT(*) FROM dm.dim_product_category
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dm.dim_product
UNION ALL
SELECT 'dim_sale_date', COUNT(*) FROM dm.dim_sale_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM dm.fact_sales
ORDER BY table_name;