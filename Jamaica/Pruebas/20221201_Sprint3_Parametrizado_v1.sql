-----------------------------------------------------------------------------------------
------------------------- SPRINT 3 PARAMETRIZADO - V1 -----------------------------------
-----------------------------------------------------------------------------------------

WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT DATE_TRUNC('month',DATE('2022-08-01')) AS input_month
)

,fmc_table AS (
SELECT * FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
WHERE month = date(dt) AND date(dt) = (SELECT input_month FROM parameters)
)
-------------------- Sales ------------------

,sales AS (
SELECT  DATE_TRUNC('month',DATE(act_acct_inst_dt)) AS month
        ,act_acct_cd
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica'
    AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
    AND act_acct_stat IN ('B','D','P','SN','SR','T','W') 
    AND DATE_TRUNC('month',DATE(act_acct_inst_dt)) = DATE_TRUNC('month', DATE(dt))
    AND DATE_TRUNC('month', DATE(dt)) = (SELECT input_month FROM parameters)   --- LINEA NUEVA ----
)

-------------------- Soft Dx + Never Paid ------------------
,first_bill AS (
SELECT  DATE_TRUNC('month',DATE(first_inst_dt)) AS month
        ,act_acct_cd
        ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
FROM    (SELECT act_acct_cd
                ,dt
                ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt
                ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE org_cntry = 'Jamaica'
            AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
            AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
            AND act_acct_cd IN (SELECT act_acct_cd FROM sales)
            AND DATE(dt) BETWEEN (DATE_TRUNC('Month', DATE(act_acct_inst_dt)) - interval '6' month) AND (DATE_TRUNC('Month', DATE(act_acct_inst_dt)) + interval '2' month)
            AND oldest_unpaid_bill_dt <> '19000101'
        )
GROUP BY 1,2
)

,max_overdue_first_bill AS (
SELECT  DATE_TRUNC('month',DATE(MIN(first_inst_dt))) as month
        ,act_acct_cd 
        ,MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) AS first_oldest_unpaid_bill_dt
        ,MIN(first_inst_dt) AS first_inst_dt, MIN(first_act_cust_strt_dt) AS first_act_cust_strt_dt
        ,CONCAT(MAX(act_acct_cd),'-',MIN(first_oldest_unpaid_bill_dt)) AS act_first_bill
        ,MAX(fi_outst_age) AS max_fi_outst_age
        ,MAX(DATE(dt)) AS max_dt
        ,MAX(CASE WHEN pd_mix_cd IS NULL THEN 0 ELSE CAST(REPLACE(pd_mix_cd,'P','') AS INT) END) AS RGUs
        ,CASE WHEN MAX(CAST(fi_outst_age AS INT)) >= 90 THEN 1 ELSE 0 END AS never_paid_flg
        ,CASE WHEN MAX(CAST(fi_outst_age AS INT)) >=36 THEN 1 ELSE 0 END AS soft_dx_flg
        ,CASE WHEN (MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval '90' day) < current_date THEN 1 ELSE 0 END AS neverpaid_window
        ,CASE WHEN (MIN(DATE(DATE_PARSE(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval '36' day) < current_date THEN 1 ELSE 0 END AS softdx_window
FROM    (SELECT act_acct_cd
                ,FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
                ,FIRST_VALUE(DATE(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt
                ,FIRST_VALUE(DATE(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt
                ,fi_outst_age
                ,dt
                ,pd_mix_cd
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE org_cntry = 'Jamaica'
            AND act_cust_typ_nm IN ('Browse & Talk HFONE', 'Residence', 'Standard')
            AND act_acct_stat IN ('B','D','P','SN','SR','T','W')
            AND concat(act_acct_cd,'-',oldest_unpaid_bill_dt) IN (SELECT act_first_bill FROM first_bill)
            AND DATE(dt) BETWEEN DATE_TRUNC('Month', DATE(act_acct_inst_dt)) AND (DATE_TRUNC('Month', DATE(act_acct_inst_dt)) + interval '5' month)
        )
GROUP BY act_acct_cd
)

,so_inst_date_search AS (
SELECT  account_id
        ,MIN(DATE(CAST(completed_date AS TIMESTAMP))) AS completed_so_dt
FROM "db-stage-dev"."so_hdr_cwc"
WHERE order_status = 'COMPLETED'
    AND order_type = 'INSTALLATION'
    AND network_type NOT IN ('LTE','MOBILE') OR network_type IS NULL
    AND CAST(account_id AS VARCHAR) IN (SELECT act_acct_cd FROM max_overdue_first_bill)
    AND DATE_TRUNC('month',DATE(order_start_date)) BETWEEN ((SELECT input_month FROM parameters) - interval '2' month) AND ((SELECT input_month FROM parameters) + interval '1' month)
GROUP BY 1
)

,final_inst_dt AS (
SELECT  *
        ,Soft_Dx_flg AS SoftDx_Flag
        ,never_paid_flg AS NeverPaid_Flag
        ,CASE WHEN completed_so_dt > first_inst_dt THEN completed_so_dt ELSE first_inst_dt END AS first_inst_dt_final
FROM max_overdue_first_bill M LEFT JOIN so_inst_date_search S ON M.act_acct_cd = CAST(S.account_id AS VARCHAR) AND M.month = DATE_TRUNC('Month',S.completed_so_dt)
)
        
,final AS (
SELECT  *
        ,CASE WHEN DATE_ADD('day',90,first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS never_paid_window_completed
        ,DATE_ADD('day',90,first_oldest_unpaid_bill_dt) AS threshold_never_paid_date
        ,CASE WHEN DATE_ADD('day',36,first_oldest_unpaid_bill_dt) < current_date THEN 1 ELSE 0 END AS soft_dx_window_completed
        ,DATE_ADD('day',36,first_oldest_unpaid_bill_dt) AS threshold_soft_dx_date
        ,current_date AS current_date_analysis
        ,DATE_TRUNC('month',DATE_ADD('day',90,first_oldest_unpaid_bill_dt)) AS never_paid_month
        ,DATE_TRUNC('month',DATE_ADD('day',36,first_oldest_unpaid_bill_dt)) AS soft_dx_month
FROM final_inst_dt
)

,final_w_fmc AS (
SELECT  F.*
        ,A.act_acct_cd
        ,A.first_oldest_unpaid_bill_dt
        ,A.first_inst_dt
        ,A.first_act_cust_strt_dt
        ,A.act_first_bill
        ,A.max_fi_outst_age
        ,A.max_dt
        ,A.RGUs
        ,A.NeverPaid_Flag
        ,A.SoftDx_Flag
        ,A.neverpaid_window
        ,A.softdx_window
        ,A.completed_so_dt
        ,A.first_inst_dt_final
        ,A.never_paid_window_completed
        ,A.threshold_never_paid_date
        ,A.soft_dx_window_completed
        ,A.threshold_soft_dx_date
        ,A.current_date_analysis
        ,A.never_paid_month
        ,A.soft_dx_month
        ,CASE WHEN A.act_acct_cd IS NOT NULL THEN 1 ELSE 0 END AS monthsale_flag
FROM fmc_table F LEFT JOIN final A ON F.fixed_account = A.act_acct_cd AND F.month = A.month
)

-------------------------- Late Installations -----------------------

,install_data AS (
SELECT  DATE_TRUNC('Month',CAST(act_cust_strt_dt AS DATE)) AS Sales_Month
        ,DATE_TRUNC('Month',CAST(act_acct_inst_dt AS DATE)) AS Install_Month
        ,act_acct_cd
        ,act_cust_strt_dt
        ,act_acct_inst_dt
        ,CASE WHEN LENGTH(act_acct_cd) = 8 THEN 'CERILLION' ELSE 'LIBERATE' END AS CRM
FROM    (SELECT *
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE DATE_TRUNC('month',DATE(dt)) BETWEEN ((SELECT input_month FROM parameters) - interval '3' month) AND (SELECT input_month FROM parameters)
        )
WHERE act_acct_cd IN (SELECT act_acct_cd FROM sales) 
)

,install_summary AS (
SELECT  sales_month 
        ,install_month
        ,act_acct_cd
        ,MAX(crm) AS crm
        ,MIN(DATE(act_cust_strt_dt)) AS act_cust_strt_dt
        ,MIN(DATE(act_acct_inst_dt)) AS act_acct_inst_dt
        ,CASE WHEN DATE_DIFF('day',MIN(DATE(act_cust_strt_dt)),MIN(DATE(act_acct_inst_dt))) >= 6 THEN 1 ELSE 0 END AS long_install_flag
FROM install_data
GROUP BY 3,1,2
)

,late_install_flag AS (
SELECT  F.*
        ,sales_month
        ,install_month
        ,long_install_flag
FROM final_w_fmc F LEFT JOIN install_summary i ON F.fixed_account = i.act_acct_cd AND F.month = i.install_month
)

------------------MRC Changes------------------------------------------------

,previous_month_base AS (
SELECT  month
        ,act_acct_cd
        ,first_due_record
        ,last_due_record
        ,last_MRC
        ,last_bundle
        ,pd_bb_prod_nm
        ,pd_vo_prod_nm
        ,pd_tv_prod_nm
FROM    (SELECT act_acct_cd
                ,date_trunc('MONTH', DATE(dt)) as month
                ,first_value(fi_outst_age) over(partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt) as first_due_record
                ,first_value(fi_outst_age) over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_due_record
                ,first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as last_MRC
                ,first_value(bundle_code) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_bundle
                ,first_value(pd_bb_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_bb_prod_nm
                ,first_value(pd_vo_prod_nm) IGNORE NULLS over(partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as pd_vo_prod_nm
                ,first_value(pd_tv_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as pd_tv_prod_nm 
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE org_cntry='Jamaica'
            AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
            AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
            AND DATE_TRUNC('month',DATE(dt)) = ((SELECT input_month FROM parameters) - interval '1' month)
        )
GROUP BY month, act_acct_cd, first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
HAVING (cast(first_due_record as int) <=90  or first_due_record is null) AND (cast(last_due_record as int) <=90 or last_due_record is null )
)

,actual_month_base AS (
SELECT  month
        ,act_acct_cd
        ,first_due_record
        ,last_due_record
        ,last_MRC
        ,last_bundle
        ,pd_bb_prod_nm
        ,pd_vo_prod_nm
        ,pd_tv_prod_nm
FROM    (SELECT act_acct_cd
                ,date_trunc('MONTH', DATE(dt)) as Month
                ,first_value(fi_outst_age) over(partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt) as first_due_record
                ,first_value(fi_outst_age) over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_due_record
                ,first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_MRC
                ,first_value(bundle_code) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_bundle
                ,first_value(pd_bb_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_bb_prod_nm
                ,first_value(pd_vo_prod_nm) IGNORE NULLS over(partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_vo_prod_nm
                ,first_value(pd_tv_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_tv_prod_nm 
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE org_cntry='Jamaica' 
            AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
            AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
            AND DATE_TRUNC('month',DATE(dt)) = (SELECT input_month FROM parameters)
        )
GROUP BY Month, act_acct_cd, first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
HAVING (cast(first_due_record as int) <=90  or first_due_record is null) AND (cast(last_due_record as int) <=90 or last_due_record is null )
)

,joint_bases AS (
SELECT  A.month AS real_month
        ,B.month
        ,A.act_acct_cd
        ,CASE WHEN B.last_MRC = 0 THEN NULL ELSE A.last_MRC / B.last_MRC END AS MRC_growth
        ,A.last_bundle AS actual_bundle
        ,B.last_bundle AS previous_bundle
        ,CONCAT(A.pd_bb_prod_nm,A.pd_vo_prod_nm,A.pd_tv_prod_nm) AS actual_plans
        ,CONCAT(B.pd_bb_prod_nm,B.pd_vo_prod_nm,B.pd_tv_prod_nm) AS previous_plans
FROM actual_month_base A LEFT JOIN previous_month_base B ON A.act_acct_cd = B.act_acct_cd AND A.Month = DATE_ADD('MONTH',1,B.Month)
ORDER BY MRC_growth DESC
)

,MRCFlag_summary AS (
SELECT   real_month
        ,act_acct_cd
        ,CASE WHEN MRC_growth > 1.05 AND (actual_bundle = previous_bundle OR actual_plans = previous_plans) THEN 1 ELSE 0 END AS increase_flag
        ,CASE WHEN (actual_bundle = previous_bundle OR actual_plans = previous_plans) THEN 1 ELSE 0 END AS no_plan_change_flag
FROM joint_bases
)

,FullTable_MRCFlag AS (
SELECT  F.*
        ,M.increase_flag
        ,M.no_plan_change_flag
FROM Late_install_flag F LEFT JOIN MRCFlag_Summary M ON F.fixed_account = M.act_acct_cd AND F.month = M.Real_Month
)

,MRCFlag_Test AS (
SELECT M.*
FROM Late_install_flag F RIGHT JOIN MRCFlag_Summary M ON F.final_account = M.act_acct_cd AND F.month = M.real_month
WHERE F.final_account IS NULL
)

------------------------------ Mounting Bills-------------------------------------
,mountingbills_initial_table AS (
SELECT  date_trunc('Month',CAST(dt AS DATE)) AS month
        ,act_acct_cd
        ,CASE WHEN fi_outst_age IS NULL THEN -1 ELSE CAST(fi_outst_age AS DOUBLE) END AS fi_outst_age
        ,CASE WHEN fi_outst_age = '60' THEN 1 ELSE 0 END AS day_60
        ,first_value(CASE WHEN fi_outst_age IS NULL THEN -1 ELSE CAST(fi_outst_age AS DOUBLE) END) IGNORE NULLS OVER(PARTITION BY date_trunc('Month',CAST(dt AS DATE)),act_acct_cd ORDER BY dt DESC) AS last_overdue_record
        ,first_value(CASE WHEN fi_outst_age IS NULL THEN -1 ELSE CAST(fi_outst_age AS DOUBLE) END) IGNORE NULLS OVER(PARTITION BY date_trunc('Month',CAST(dt AS DATE)),act_acct_cd ORDER BY dt) AS first_overdue_record
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica' 
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
    AND CAST(dt AS DATE) BETWEEN date_trunc('MONTH', CAST(dt AS DATE)) AND date_add('MONTH',1,date_trunc('MONTH',CAST(dt AS DATE)))
),

FinalMountingBills AS (
SELECT  month
        ,act_acct_cd
        ,MAX(fi_outst_age) AS max_overdue
        ,MAX(day_60) AS day_60
        ,MAX(last_overdue_record) AS last_overdue_record
        ,MAX(first_overdue_record) AS first_overdue_record
FROM mountingbills_initial_table
GROUP BY 1,2
HAVING MAX(last_overdue_record) <= 90 AND MAX(first_overdue_record) <= 90 
)

,FullTable_MountBills_Flag AS (
SELECT  F.*
        ,day_60 AS mountingbill_flag
FROM FullTable_MRCFlag F LEFT JOIN FinalMountingBills B ON F.fixed_account = B.act_acct_cd AND F.month= B.Month
)
------------------------- Early Tickets ---------------------------------------
,clean_interactions_base AS (
SELECT *, row_number() OVER(PARTITION BY REGEXP_REPLACE(account_id,'[^0-9 ]',''),CAST(interaction_start_time AS DATE) ORDER BY interaction_start_time DESC) AS row_num
FROM "db-stage-dev"."interactions_cwc"
WHERE lower(org_cntry) LIKE '%jam%'
    AND DATE_TRUNC('month',DATE(interaction_start_time)) = (SELECT input_month FROM parameters)
)

,initial_table_tickets AS (
SELECT  DATE(date_trunc('MONTH', interaction_start_time)) AS Ticket_Month
        ,interaction_id
        ,account_id_2
        ,MIN(DATE(interaction_start_time)) AS interaction_start_time
FROM    (SELECT *, REGEXP_REPLACE(account_id,'[^0-9 ]','') AS account_id_2
        FROM (SELECT * FROM clean_interactions_base HAVING row_num = 1)
        WHERE lower(org_cntry) LIKE '%jam%'
        ) 
GROUP BY 1,2,3
)

,installations AS (
SELECT  date_trunc('Month', min(date(act_cust_strt_dt))) AS Sales_Month
        ,act_acct_cd
        ,MIN(DATE(act_cust_strt_dt)) AS act_cust_strt_dt
        ,MIN(DATE(act_acct_inst_dt)) AS act_acct_inst_dt
        ,date_trunc('MONTH',MIN(DATE(act_acct_inst_dt))) AS Inst_Month
FROM "db-analytics-prod"."tbl_fixed_cwc"
WHERE org_cntry = 'Jamaica'
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
    AND DATE(act_cust_strt_dt) BETWEEN date_trunc('Month',cast(act_cust_strt_dt AS DATE)) AND date_add('DAY',90,date_trunc('Month',CAST(act_cust_strt_dt AS DATE)))
    --AND DATE_TRUNC('month',DATE(act_cust_strt_dt)) BETWEEN ((SELECT input_month FROM parameters) - INTERVAL '6' MONTH) AND ((SELECT input_month FROM parameters) + INTERVAL '0' MONTH)
GROUP BY act_acct_cd
)

,joint_bases_et AS (
SELECT  T.*
        ,i.sales_month
        ,i.act_cust_strt_dt
        ,i.inst_month
        ,i.act_acct_inst_dt
FROM initial_table_tickets T LEFT JOIN installations i ON T.account_id_2 = i.act_acct_cd
)

,account_summary_tickets AS (
SELECT   account_id_2 AS ACCOUNT_ID
        ,MAX(CASE WHEN DATE_DIFF('week',act_acct_inst_dt,interaction_start_time) <= 7 THEN 1 ELSE 0 END) AS early_tickets
        ,Sales_Month
        ,Inst_Month
        ,date_trunc('MONTH', interaction_start_time) AS Ticket_Month
FROM joint_bases_et
GROUP by Sales_Month, account_id_2, Inst_Month,5
)

,FullTable_EarlyTickets AS (
SELECT  F.*
        ,early_tickets AS EarlyTicket_Flag
        ,ticket_month
FROM FullTable_MountBills_Flag F LEFT JOIN account_summary_tickets B ON F.fixed_account = B.Account_ID AND F.month= B.Ticket_Month
)

,saleschannel_SO AS (
SELECT  date_trunc('Month',MIN(DATE(order_start_date))) AS month
        ,date_trunc('Month',MIN(DATE(completed_date))) AS Inst_Month
        ,MAX(channel_type) AS sales_channel
        ,account_id
FROM "db-stage-dev"."so_hdr_cwc"
WHERE org_cntry = 'Jamaica' 
    AND network_type NOT IN ('LTE','MOBILE')
    AND order_status = 'COMPLETED'
    AND account_type = 'Residential'
    AND order_type = 'INSTALLATION'
    AND cease_reason_group IS NULL
    AND date_trunc('Month',DATE(order_start_date)) = (SELECT input_month FROM parameters)
GROUP BY account_id
)

,FullTable_SalesChannel AS (
SELECT  F.* 
------ Modificar clasificación de canal en caso de que aparezca Affiliate sales ----
        ,CASE   WHEN sales_channel IN ( 'DEALERS', 'RETAIL') THEN 'RETAIL'
                WHEN sales_channel IN ('ECOMMERCE', 'VIRTUAL SALES') THEN 'DIGITAL'
                WHEN sales_channel IN ('D2D') THEN sales_channel
                WHEN sales_channel IN ('TELESALES' ) THEN 'TELESALES_INBOUND'
                WHEN sales_channel IN ('CUSTOMER CARE', 'TECH SUPPORT', 'CALL CENTER', 'RETENTION DEPARMENT', 'ORDER MANAGEMENT', 'PROCESSING', 'DISPATCH') THEN 'TELESALES_OUTBOUND'
                END AS Sales_channel
        ,CASE WHEN S.account_id IS NOT NULL THEN 'Match' ELSE 'No match' 
                END AS saleschannel_flag
FROM FullTable_EarlyTickets F LEFT JOIN saleschannel_SO S ON F.fixed_account = CAST(S.account_id AS VARCHAR) AND F.install_month = S.inst_month
)

,FullTable_KPIsFlags AS (
SELECT  *
        ,CASE WHEN Monthsale_flag = 1 THEN CONCAT(CAST(Monthsale_flag AS VARCHAR),fixed_account) ELSE NULL END AS F_SalesFlag
        ,CASE WHEN SoftDx_flag = 1 THEN CONCAT(CAST(SoftDx_Flag AS VARCHAR),fixed_account) ELSE NULL END AS F_SoftDxFlag
        ,CASE WHEN NeverPaid_flag = 1 THEN CONCAT(CAST(NeverPaid_Flag AS VARCHAR),Fixed_Account) ELSE NULL END AS F_NeverPaidFlag
        ,CASE WHEN Long_install_flag = 1 THEN CONCAT(CAST(long_install_flag AS VARCHAR),Fixed_account) ELSE NULL END AS F_LongInstallFlag
        ,CASE WHEN Increase_flag = 1 THEN CONCAT(CAST(increase_flag AS VARCHAR),Fixed_Account) ELSE NULL END AS F_MRCIncreases
        ,CASE WHEN No_plan_change_flag = 1 THEN CONCAT(CAST(no_plan_change_flag AS VARCHAR),fixed_account) ELSE NULL END AS F_NoPlanChangeFlag
        ,CASE WHEN Mountingbill_flag = 1 THEN CONCAT(CAST(mountingbill_flag AS VARCHAR),fixed_account) ELSE NULL END AS F_MountingBillFlag
        ,CASE WHEN EarlyTicket_Flag = 1 THEN CONCAT(CAST(earlyticket_flag AS VARCHAR),fixed_account) ELSE NULL END AS F_EarlyTicketFlag
From FullTable_SalesChannel
)

,Results_Table AS (
SELECT  month AS ActiveBase_Month
        ,E_Final_Tech_Flag
        ,E_FMC_Segment
        ,E_FMCType
        ,E_FinalTenureSegment
        ,COUNT(DISTINCT fixed_account) AS activebase
        ,sales_channel
        ,SUM(monthsale_flag) AS Sales
        ,SUM(SoftDx_Flag) AS Soft_Dx
        ,SUM(NeverPaid_Flag) AS NeverPaid
        ,SUM(long_install_flag) AS Long_installs
        ,SUM(increase_flag) AS MRC_Increases
        ,SUM(no_plan_change_flag) AS NoPlan_Changes
        ,SUM(mountingbill_flag) AS MountingBills
        ,SUM(earlyticket_flag) AS EarlyTickets
        ,Sales_Month
        ,Install_Month
        ,Ticket_Month
        ,COUNT(DISTINCT F_SalesFlag) AS Unique_Sales
        ,COUNT(DISTINCT F_SoftDxFlag) AS Unique_SoftDx
        ,COUNT(DISTINCT F_NeverPaidFlag) AS Unique_NeverPaid
        ,COUNT(DISTINCT F_LongInstallFlag) AS Unique_LongInstall
        ,COUNT(DISTINCT F_MRCIncreases) AS Unique_MRCIncrease
        ,COUNT(DISTINCT F_NoPlanChangeFlag) AS Unique_NoPlanChanges
        ,COUNT(DISTINCT F_MountingBillFlag) AS Unique_Mountingbills
        ,COUNT(DISTINCT F_EarlyTicketFlag) AS Unique_EarlyTickets
FROM FullTable_KPIsFlags
WHERE finalchurnflag <> 'Fixed Churner'
    AND waterfall_flag <> 'Downsell-Fixed Customer Gap'
    AND waterfall_flag <> 'Fixed Base Exception'
    AND mainmovement <> '6.Null last day'
    AND waterfall_flag <> 'Churn Exception'
    AND month = DATE(dt)
GROUP BY 1,2,3,4,5,7,16,17,18
ORDER BY 1,2,3,4,5,7,16,17,18
)

SELECT *
FROM Results_Table

/*
SELECT ActiveBase_Month
,sum(activebase) as activebase
,sum(Sales) as sales
,sum(Soft_Dx) as soft_dx
,sum(NeverPaid) as neverpaid
,sum(Long_installs) as longinstalls
,sum(MRC_Increases) as MRC_increases
,sum(NoPlan_Changes) as no_plan
,sum(MountingBills) as mountingbills
,sum(EarlyTickets) as earlytix
,sum(Unique_Sales) as unique_sales
,sum(Unique_SoftDx) as unique_softdx
,sum(Unique_NeverPaid) as unique_neverpaid
,sum(Unique_LongInstall) as unique_longinstalls
,sum(Unique_MRCIncrease) as unique_MRC_increase
,sum(Unique_NoPlanChanges) as unique_noplanchanges
,sum(Unique_Mountingbills) as unique_mountingbills
,sum(Unique_EarlyTickets) as unique_earlytix
From Results_Table
group by 1
ORDER BY 1
*/
