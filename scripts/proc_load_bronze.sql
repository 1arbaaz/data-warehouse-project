/*
===============================================================================
Procedure: Load Bronze Layer 
Script Purpose:
    Loads CSV data into the 'bronze' schema.
    - Truncates tables
    - Loads data using COPY
    - Prints duration for each load
    - Handles errors
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    batch_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    ---------------------------------------------------------------------
    -- CRM Tables
    ---------------------------------------------------------------------
    -- crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    ---------------------------------------------------------------------
    -- ERP Tables
    ---------------------------------------------------------------------
    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/media/arbaz/caa37fa5-bd0b-489e-8086-9bc7a4dae3eb/home/arbaz/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := clock_timestamp();
    RAISE NOTICE 'erp_px_cat_g1v2 loaded in % seconds',
                 EXTRACT(SECOND FROM end_time - start_time);

    ---------------------------------------------------------------------
    -- Finish
    ---------------------------------------------------------------------
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze Load Completed in % seconds',
                 EXTRACT(SECOND FROM clock_timestamp() - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR while loading bronze layer: %', SQLERRM;
END;
$$;
