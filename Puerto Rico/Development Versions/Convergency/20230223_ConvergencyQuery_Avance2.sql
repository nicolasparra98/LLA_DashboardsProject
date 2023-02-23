-----------------------------------------------------------------------------------------
-------------------------- LCPR CONVERGENCY QUERY - V1 ----------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_convergency_jan_feb23" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
)

,fixed_useful_fields AS (
SELECT  date(dt) AS fixed_dt
        ,sub_acct_no_sbb as fixed_account
        ,home_phone_sbb AS fixed_phone1
        ,bus_phone_sbb AS fixed_phone2
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH)
)

,mobile_useful_fields_BOM AS (
SELECT  date_add('month',1,DATE_TRUNC('month',DATE(dt))) AS mobile_dt
        ,cust_id AS mobile_account
        ,subsrptn_id AS mobile_phone1
        ,cust_hm_phn AS mobile_phone2
        ,cust_contct_phn AS mobile_phone3
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
)

,mobile_useful_fields_EOM AS (
SELECT  DATE_TRUNC('month',DATE(dt)) AS mobile_dt
        ,cust_id AS mobile_account
        ,subsrptn_id AS mobile_phone1
        ,cust_hm_phn AS mobile_phone2
        ,cust_contct_phn AS mobile_phone3
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters)
)

,BOM_convergency AS (
SELECT  mobile_dt AS month
        ,fixed_account
        ,mobile_account
FROM    (
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone1 = B.fixed_phone1) 
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone1 = B.fixed_phone2)
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone2 = B.fixed_phone1) 
        /*
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone2 = B.fixed_phone2) 
    */
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone3 = B.fixed_phone1) 
    /*
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_BOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',b.fixed_dt) AND (A.mobile_phone3 = B.fixed_phone2) 
    */
        )
WHERE fixed_account IS NOT NULL
GROUP BY 1,2,3
)

,EOM_convergency AS (
SELECT  mobile_dt AS month
        ,fixed_account
        ,mobile_account
FROM    (
    SELECT  A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone1 = B.fixed_phone1) 
    UNION ALL
    SELECT A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone1 = B.fixed_phone2)
    UNION ALL
    SELECT  A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone2 = B.fixed_phone1)
    /*
    UNION ALL
    SELECT  A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone2 = B.fixed_phone2)
    */
    UNION ALL
    SELECT  A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone3 = B.fixed_phone1)
        /*
    UNION ALL
    SELECT  A.*,fixed_account,fixed_phone1,fixed_phone2
    FROM mobile_useful_fields_EOM A LEFT JOIN (SELECT * FROM fixed_useful_fields WHERE fixed_dt = DATE_TRUNC('MONTH',fixed_dt)) B
        ON A.mobile_dt = DATE_TRUNC('MONTH',(b.fixed_dt - interval '1' day)) AND (A.mobile_phone3 = B.fixed_phone2)
        */
        )
WHERE fixed_account IS NOT NULL
GROUP BY 1,2,3
)

,full_convergency AS (
SELECT month, fixed_account, mobile_account
FROM BOM_convergency
UNION ALL
SELECT month, fixed_account, mobile_account
FROM EOM_convergency
)

,clean_convergency as (
SELECT month, fixed_account, mobile_account, row_number() OVER (PARTITION BY fixed_account ORDER BY mobile_account desc) as row_num
FROM full_convergency 
group by 1,2,3
order by 1,2,3,4
)

SELECT month, fixed_account, mobile_account
FROM clean_convergency
WHERE row_num = 1
