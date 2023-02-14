-----------------------------------------------------------------------------------------
---------------------------- LCPR FIXED TABLE - V2 --------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_Prueba2SEPT_jul" AS

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
        ,85 as overdue_days
)

,useful_fields AS (
SELECT  date(dt) AS dt
        ,DATE(as_of) AS as_of
        ,sub_acct_no_sbb AS fix_s_att_account
        ,home_phone_sbb AS fix_s_att_phone
        ,delinquency_days AS overdue
        ,(CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) AS max_start
        ,(video_chrg + hsd_chrg + voice_chrg) AS TOTAL_MRC
        ,IF(TRIM(drop_type) = 'FIBER','FTTH',IF(TRIM(drop_type) = 'FIBCO','HFC',IF(TRIM(drop_type) = 'COAX','COPPER',null))) AS tech_flag
        ,hsd AS numBB
        ,video AS numTV
        ,voice AS numVO
        ,null AS oldest_unpaid_bill_dt
        ,first_value(delinquency_days) over(PARTITION BY sub_acct_no_sbb,date_trunc('month',date(dt)) ORDER BY date(dt) DESC) AS last_overdue
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN ((SELECT input_month FROM parameters) + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  ((SELECT input_month FROM parameters) + interval '1' MONTH) 
)

,BOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',dt) AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_b_dim_date
        ,tech_flag AS fix_b_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_b_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_b_fla_MixNameAdj
        ,NULL AS fix_b_att_bbCode
        ,NULL AS fix_b_att_tvCode
        ,NULL AS fix_b_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_b_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_b_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_b_fla_VO
        ,(numBB + numTV + numVO) AS fix_b_mes_numRGUS
        ,total_MRC AS fix_b_mes_mrc
        ,overdue AS fix_b_mes_overdue
        ,max_start AS fix_b_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_b_fla_tenure
FROM useful_fields
WHERE dt = (SELECT input_month FROM parameters)
    AND (CAST(overdue AS INTEGER) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,EOM_active_base AS (
SELECT  DATE_TRUNC('MONTH',(dt - interval '1' day))  AS fix_s_dim_month
        ,fix_s_att_account
        ,dt AS fix_e_dim_date
        ,tech_flag AS fix_e_fla_tech
        ,CONCAT(CAST((numBB+numTV+numVO) AS VARCHAR),'P') AS fix_e_fla_MixCodeAdj
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS fix_e_fla_MixNameAdj
        ,NULL AS fix_e_att_bbCode
        ,NULL AS fix_e_att_tvCode
        ,NULL AS fix_e_att_voCode
        ,IF(numBB = 1,fix_s_att_account,NULL) AS fix_e_fla_BB
        ,IF(numTV = 1,fix_s_att_account,NULL) AS fix_e_fla_TV
        ,IF(numVO = 1,fix_s_att_account,NULL) AS fix_e_fla_VO
        ,(numBB + numTV + numVO) AS fix_e_mes_numRGUS
        ,total_MRC AS fix_e_mes_mrc
        ,overdue AS fix_e_mes_overdue
        ,max_start AS fix_e_att_MaxStart
        ,CASE   WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 180 THEN 'Early-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 180 AND DATE_DIFF('DAY', Max_Start, DATE(dt)) <= 360 THEN 'Mid-Tenure'
                WHEN DATE_DIFF('DAY', Max_Start, DATE(dt)) > 360 THEN 'Late-Tenure'
                    END AS fix_e_fla_tenure
FROM useful_fields
WHERE dt = (SELECT input_month FROM parameters) + interval '1' month
    AND (CAST(overdue AS INTEGER) < (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,customer_status AS (
SELECT  CASE    WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_dim_month
                WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_dim_month
                    END AS fix_s_dim_month
        ,CASE WHEN (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NOT NULL) OR (A.fix_s_att_account IS NOT NULL AND B.fix_s_att_account IS NULL) THEN A.fix_s_att_account
        WHEN (A.fix_s_att_account IS NULL AND B.fix_s_att_account IS NOT NULL) THEN B.fix_s_att_account
                    END AS fix_s_att_account
        ,IF(A.fix_s_att_account IS NOT NULL,1,0) AS fix_b_att_active
        ,IF(B.fix_s_att_account IS NOT NULL,1,0) AS fix_e_att_active
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_MaxStart
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_mes_numRGUS
        ,fix_b_fla_MixNameAdj
        ,fix_b_fla_MixCodeAdj
        ,fix_b_fla_BB
        ,fix_b_fla_TV
        ,fix_b_fla_VO
        ,fix_b_att_bbCode
        ,fix_b_att_tvCode
        ,fix_b_att_voCode
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_MaxStart
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_mes_numRGUS
        ,fix_e_fla_MixNameAdj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_fla_BB
        ,fix_e_fla_TV
        ,fix_e_fla_VO
        ,fix_e_att_bbCode
        ,fix_e_att_tvCode
        ,fix_e_att_voCode
FROM BOM_active_base A FULL OUTER JOIN EOM_active_base b 
    ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.fix_s_dim_month
)

,main_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) = 0 THEN '1.SameRGUs'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) > 0 THEN '2.Upsell'
                WHEN (fix_e_mes_numRGUS - fix_b_mes_numRGUS) < 0 THEN '3.Downsell'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) =  fix_s_dim_month) THEN '4.New Customer'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) <> fix_s_dim_month) THEN '5.Come Back to Life'
                WHEN (fix_b_mes_numRGUS > 0 AND fix_e_mes_numRGUS IS NULL) THEN '6.Null last day'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS IS NULL) THEN '7.Always null'
                WHEN (fix_b_mes_numRGUS IS NULL AND fix_e_mes_numRGUS > 0 AND DATE_TRUNC ('MONTH', fix_e_att_MaxStart) is null) THEN '8.Rejoiner-GrossAdd Gap'
                    END AS fix_s_fla_MainMovement
FROM customer_status A
)

,spin_movement_flag AS (
SELECT  A.*
        ,CASE   WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) > 0 THEN '1. Up-spin'
                WHEN fix_s_fla_MainMovement = '1.SameRGUs' AND (fix_e_mes_mrc - fix_b_mes_mrc) < 0 THEN '2. Down-spin'
                ELSE '3. No Spin' 
                    END AS fix_s_fla_SpinMovement
FROM main_movement_flag A
)

--------------------------------- VOLUNTARY CHURN ---------------------------------------
,service_orders_flag AS (
SELECT  DATE_TRUNC('MONTH',DATE(completed_date)) AS month
        ,DATE(completed_date) AS end_date
        ,DATE(order_start_date) AS start_date
        ,cease_reason_code
        ,cease_reason_desc
        ,order_type
        ,CASE   WHEN cease_reason_desc = 'MIG COAX TO FIB' THEN 'Migracion'
                WHEN cease_reason_desc = 'NON-PAY' THEN 'Involuntario'
                ELSE 'Voluntario' END AS dx_type
        ,account_id
        ,lob_vo_count
        ,lob_bb_count
        ,lob_tv_count
        ,IF(lob_vo_count > 0,1,0) AS VO_Churn
        ,IF(lob_bb_count > 0,1,0) AS BB_Churn
        ,IF(lob_tv_count > 0,1,0) AS TV_Churn
        ,(IF(lob_vo_count > 0,1,0)+IF(lob_bb_count > 0,1,0)+IF(lob_tv_count > 0,1,0)) AS RGUs_Prel
FROM "lcpr.stage.prod"."so_hdr_lcpr"
WHERE order_type = 'V_DISCO'
    AND account_type = 'RES'
    AND order_status = 'COMPLETE'
)

,churned_RGUs_SO AS (
SELECT  month
        ,account_id
        ,dx_type
        ,SUM(RGUs_Prel) AS churned_RGUs
FROM service_orders_flag
GROUP BY 1,2,3
)

,RGUS_MixLastDay AS (
SELECT  DATE_TRUNC('MONTH',dt) AS month
        ,dt
        ,fix_s_att_account
        ,overdue
        ,CASE   WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BB'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
                WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BB+TV'
                WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BB+VO'
                WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
                WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BB+VO+TV'
                    END AS MixName_Adj
FROM useful_fields
)

,RGUs_LastRecord_DNA AS (
SELECT  DISTINCT month
        ,fix_s_att_account
        ,first_value(MixName_Adj) OVER (PARTITION BY fix_s_att_account,month ORDER BY dt DESC) AS last_RGU
FROM RGUS_MixLastDay
WHERE (cast(overdue as double) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,RGUS_LastRecord_DNA_Adj AS (
SELECT  month
        ,fix_s_att_account
        ,last_RGU
        ,CASE   WHEN last_RGU IN ('VO','BB','TV') THEN 1
                WHEN last_RGU IN ('BB+VO', 'BB+TV', 'VO+TV') THEN 2
                WHEN last_RGU IN ('BB+VO+TV') THEN 3
                WHEN last_RGU IN ('0P') THEN -1
                    ELSE 0 END AS NumRGUs_LastRecord
FROM RGUs_LastRecord_DNA
)

,date_LastRecord_DNA AS (
SELECT  DATE_TRUNC('MONTH',dt) AS month
        ,fix_s_att_account
        ,MAX(dt) AS last_date
FROM useful_fields
WHERE (cast(overdue as double) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
GROUP BY 1,2
)

,overdue_LastRecord_DNA AS (
SELECT  DATE_TRUNC('MONTH',A.dt) AS month
        ,A.fix_s_att_account
        ,A.overdue AS LastOverdueRecord
        ,(DATE_DIFF('DAY',DATE(A.max_start),A.dt)) AS ChurnTenureDays
FROM useful_fields A INNER JOIN date_LastRecord_DNA B ON A.fix_s_att_account = B.fix_s_att_account AND A.dt = B.last_date
)

,voluntary_flag AS(
SELECT  B.month
        ,B.fix_s_att_account
        ,A.dx_type
        ,B.Last_RGU
        ,B.NumRGUs_LastRecord
        ,A.churned_RGUs
        ,IF(A.churned_RGUs >= B.NumRGUs_LastRecord,1,0) AS vol_flag
FROM churned_RGUs_SO A
INNER JOIN RGUs_LastRecord_DNA_Adj B    ON A.account_id = B.fix_s_att_account AND A.month = B.month
INNER JOIN date_LastRecord_DNA C        ON B.fix_s_att_account = C.fix_s_att_account AND B.month = date_trunc('month',C.last_date)
INNER JOIN overdue_LastRecord_DNA D     ON B.fix_s_att_account = D.fix_s_att_account AND B.month = D.month
)

,voluntary_churners AS (
SELECT  DISTINCT A.fix_s_dim_month
        ,A.fix_s_att_account
        ,A.fix_b_mes_numRGUS
        ,A.fix_e_att_active
        ,B.last_RGU
        ,B.churned_RGUs
        ,B.NumRGUs_LastRecord
        ,IF(B.fix_s_att_account IS NOT NULL AND B.vol_flag = 1,'Voluntario',null) AS ChurnType
FROM spin_movement_flag A LEFT JOIN voluntary_flag B ON A.fix_s_att_account = B.fix_s_att_account AND A.fix_s_dim_month = B.month
)

,voluntary_churners_adj AS (
SELECT  DISTINCT fix_s_dim_month AS month
        ,fix_s_att_account AS churn_account
        ,ChurnType
        ,fix_e_att_active
        ,IF(ChurnType IS NOT NULL AND fix_e_att_active = 1 AND fix_b_mes_numRGUS > NumRGUs_LastRecord,1,0) AS partial_churn
FROM voluntary_churners
)

,final_voluntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,IF(churn_account IS NOT NULL AND (fix_e_att_active = 0 OR fix_e_att_active IS NULL),'1. Fixed Voluntary Churner',NULL) AS fix_s_fla_ChurnType
FROM voluntary_churners_adj
WHERE ChurnType IS NOT NULL AND partial_churn = 0 
)

-------------------------------- INVOLUNTARY CHURN --------------------------------------
,invol_churners_workaround AS (
SELECT  fix_s_att_account
        ,date_trunc('month',max(dt)) AS month
        ,max(dt) AS last_date
        ,last_overdue
        ,(numBB+numTV+numVO) AS total_RGUs
FROM useful_fields
WHERE dt BETWEEN (SELECT input_month FROM parameters) AND (SELECT input_month FROM parameters) + interval '1' month - interval '1' day
GROUP BY 1,4,5
)

,involuntary_churners AS (
SELECT  month
        ,fix_s_att_account AS churn_account
        ,last_overdue
        ,last_date
        ,'2. Fixed Involuntary Churner' AS fix_s_fla_ChurnType
FROM invol_churners_workaround 
where last_overdue >= (SELECT overdue_days FROM parameters)
GROUP BY 1,2,4,3
)

,final_involuntary_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM involuntary_churners
WHERE fix_s_fla_ChurnType = '2. Fixed Involuntary Churner'
)

,all_churners AS (
SELECT  DISTINCT month
        ,churn_account
        ,fix_s_fla_ChurnType
FROM    (SELECT month,churn_account,fix_s_fla_ChurnType FROM final_voluntary_churners A 
         UNION ALL
         SELECT month,churn_account,fix_s_fla_ChurnType FROM final_involuntary_churners B
        )
)

,fixed_table_churn_flag AS(
SELECT  A.*
        ,CASE   WHEN B.churn_account IS NOT NULL THEN '1. Fixed Churner'
                WHEN B.churn_account IS NULL THEN '2. Fixed NonChurner'
                    END AS fix_s_fla_ChurnFlag
        ,CASE   WHEN B.churn_account IS NOT NULL THEN fix_s_fla_ChurnType
                    END AS fix_s_fla_ChurnType
FROM spin_movement_flag A LEFT JOIN all_churners B ON A.fix_s_att_account = B.churn_account AND A.fix_s_dim_month = B.month
)

select distinct fix_s_dim_month, fix_s_fla_MainMovement,fix_s_fla_SpinMovement,fix_s_fla_ChurnFlag,fix_s_fla_ChurnType, count(distinct fix_s_att_account) as accounts, sum(fix_b_mes_numRGUS) as BOM_RGUs,sum(fix_e_mes_numRGUS) as BOM_RGUs
from fixed_table_churn_flag
group by 1,2,3,4,5
order by 1,2,3,4,5


--select * from fixed_table_churn_flag limit 200

-------------------------------- FMC AND REJOINERS --------------------------------------
