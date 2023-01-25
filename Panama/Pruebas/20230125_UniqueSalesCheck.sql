with 
Parameters AS (
    SELECT
        DATE('2022-08-01') AS month_analysis
        ,5 as ovr_due_thr
    )

---------------------------- SERVICE ORDERS -----------------------------------------------------------------------------------

,service_orders AS (
SELECT  *
        ,DATE_TRUNC('Month', DATE(order_start_date)) AS month
        ,DATE(order_start_date) AS StartDate
        ,DATE(completed_date) AS EndDate
        ,DATE_DIFF('DAY',DATE(order_start_date),DATE(completed_date)) AS installation_lapse
FROM "db-stage-dev"."so_hdr_cwp"
WHERE order_type = 'INSTALLATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
        AND DATE_TRUNC('MONTH',CAST(order_start_date AS DATE)) between (SELECT month_analysis FROM parameters) and (SELECT month_analysis FROM parameters) + interval '1' month
)

---------------------------- CB ANALYSIS -----------------------------------------------------------------------------------

,dna_cwp_fixed AS (
        select *,
        CASE WHEN DATE(act_acct_inst_dt) < DATE('2022-08-01') THEN CAST(fi_outst_age AS bigint)
        ELSE 
                date_diff('day',date(date_parse(substring(cast(oldest_unpaid_bill_dt as varchar),1,8), '%Y%m%d')),date(dt)) 
        END AS fi_outst_age2
        from "db-analytics-prod"."fixed_cwp"
        where act_cust_typ_nm = 'Residencial'
        AND DATE_TRUNC('MONTH', DATE(dt)) BETWEEN (SELECT month_analysis FROM Parameters) AND (SELECT month_analysis FROM Parameters) + INTERVAL '2' MONTH
)
,active_users AS (
SELECT DISTINCT act_acct_cd
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND (DATE(dt) BETWEEN (SELECT month_analysis FROM Parameters) - INTERVAL '6' MONTH AND  (SELECT month_analysis FROM Parameters) - INTERVAL '1' DAY
OR DATE_TRUNC('MONTH', DATE(act_acct_inst_dt)) BETWEEN (SELECT month_analysis FROM Parameters) - INTERVAL '6' MONTH AND  (SELECT month_analysis FROM Parameters) - INTERVAL '1' MONTH
)
)
,sales_base AS (
SELECT *
FROM (
    SELECT  dn.act_acct_cd, 
            instalation_date
            ,FIRST_VALUE(CAST(dn.fi_outst_age2 AS bigint)) OVER (PARTITION BY dn.act_acct_cd ORDER BY CAST(dn.fi_outst_age2 AS bigint) DESC) AS max_outst_age
    FROM dna_cwp_fixed AS dn
    INNER JOIN (
        SELECT DISTINCT ACT_ACCT_CD, 
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt ASC) AS instalation_date
        FROM "db-analytics-prod"."fixed_cwp"
        WHERE act_cust_typ_nm = 'Residencial'
        AND DATE_TRUNC('MONTH', DATE(act_acct_inst_dt)) = (SELECT month_analysis FROM Parameters) 
        AND act_acct_cd NOT IN (SELECT act_acct_cd FROM active_users)
        -- and (cast(fi_outst_age as bigint) < 90 or fi_outst_age is null)
    ) AS inst ON inst.act_acct_cd=dn.act_acct_cd
)
WHERE (max_outst_age < 90 OR max_outst_age IS NULL)
)

,CB_sales_approach as (
select date_trunc('month',instalation_date), act_acct_cd
from sales_base
)


---------------------------- OVAL ANALYSIS -----------------------------------------------------------------------------------

,fmc_table AS (
SELECT * FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
WHERE month = DATE(dt) AND month = (SELECT month_analysis FROM parameters)
)

,previous_months_dna AS (
-- Se guardan las cuentas que aparecen en los 3 meses anteriores
SELECT  DATE_TRUNC('month',CAST(dt AS DATE)) AS month
        ,act_acct_cd
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        AND DATE_TRUNC('month',DATE(dt)) BETWEEN ((SELECT month_analysis FROM parameters) - interval '3' month) AND ((SELECT month_analysis FROM parameters) - interval '1' month)
GROUP BY 1,2
ORDER BY 1,2
)

,new_customers AS (
-- Se seleccionan los usuarios que aparecen en el current month pero no en los tres anteriores para flagearlos como new customers
SELECT  act_acct_cd
        ,DATE(dt) AS dt
        ,DATE_TRUNC('MONTH',CAST(dt AS DATE)) AS month_load
        ,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start
        ,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu
        ,max(act_acct_inst_dt) AS act_acct_inst_dt 
        ,max(act_cust_strt_dt) AS act_cust_strt_dt
        ,1 AS new_customer
        ,pd_bb_accs_media
        ,pd_tv_accs_media
        ,pd_vo_accs_media
FROM "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        AND act_acct_cd  NOT IN (SELECT DISTINCT act_acct_cd FROM previous_months_dna)
        AND DATE_TRUNC('month',CAST(dt AS DATE)) = (SELECT month_analysis FROM parameters)
GROUP BY act_acct_cd, 2, DATE_TRUNC('MONTH',CAST(dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT),1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
)

,new_customers_flag AS (
SELECT  F.*
        ,A.new_customer 
        ,CASE   WHEN F.first_sales_chnl_bom IS NOT NULL AND F.first_sales_chnl_eom IS NOT NULL THEN F.first_sales_chnl_eom
                WHEN F.first_sales_chnl_bom IS NULL AND F.first_sales_chnl_eom is not null then F.first_sales_chnl_eom
                WHEN F.first_sales_chnl_eom IS NULL AND F.first_sales_chnl_bom is not null then F.first_sales_chnl_bom
                END as sales_channel
        ,CASE   WHEN a.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS monthsale_flag
FROM fmc_table F LEFT JOIN new_customers A ON F.finalaccount = A.act_acct_cd AND F.month = A.month_load
)

,oval_sales_approach as (
select fixedaccount
from new_customers_flag
WHERE monthsale_flag = 1 
)

,CB_not_recognized_users as (
select distinct fixedaccount
from oval_sales_approach 
-- where fixedaccount in (select distinct act_acct_cd from cb_sales_approach)  -- 7.378 de los 8.532 de oval, los reconocemos los dos
where fixedaccount not in (select distinct act_acct_cd from cb_sales_approach)   -- 1.160 de los 8.532 de oval, no los reconoce Carlos
)

/*    -- ESPACIO PARA VERIFICAR EN EL DNA QUE PASA CON LOS QUE NO CRUZAN entre CB y OVAL
,sample as(
select distinct act_acct_cd as cuentas, dt, act_acct_inst_dt,FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt ASC) AS instalation_date
from "db-analytics-prod"."fixed_cwp"
where act_acct_cd in (select distinct fixedaccount from CB_not_recognized_users)
order by 1,2
)

--,sample2 as (
--select instalation_date, count(distinct cuentas)
select cuentas, dt, act_acct_inst_dt, instalation_date
from sample
where date(dt) >= date('2022-08-30')
group by 1,2,3,4
order by 1,2,3,4
)
*/

    -- ESPACIO PARA VERIFICAR EN EL DNA QUE PASA CON LOS QUE YO RECONOZCO QUE NO TIENEN OS EN SEPTIEMBRE O EN OCTUBRE

,SAMPLE AS (
--SELECT count(distinct fixedaccount)
select distinct fixedaccount
from oval_sales_approach 
where fixedaccount not in (select distinct cast(account_id as varchar) from service_orders)
)

,sample2 as(
select distinct act_acct_cd as cuentas, dt, act_acct_inst_dt,FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY act_acct_inst_dt ASC) AS instalation_date
from "db-analytics-prod"."fixed_cwp"
where act_acct_cd in (select distinct fixedaccount from sample)
order by 1,2
)

SELECT count(distinct fixedaccount) from sample
--select * from sample2
