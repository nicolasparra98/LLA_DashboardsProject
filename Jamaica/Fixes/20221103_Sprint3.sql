----------------------------------------------------------------------------------------
-------------------- JAMAICA - SPRINT 3 (ALEX) -----------------------------------------
----------------------------------------------------------------------------------------

--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix" AS

WITH 

FMC_Table AS( 
SELECT * FROM  "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" 
)

-------------------- Sales ------------------

,sales as (
SELECT DATE_TRUNC('month',date(act_acct_inst_dt)) as Month,act_acct_cd
        FROM "db-analytics-prod"."tbl_fixed_cwc"
        WHERE org_cntry = 'Jamaica'
            AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and DATE_TRUNC('month',date(act_acct_inst_dt)) = DATE_TRUNC('month', date(dt))
)

-------------------- Soft Dx + Never Paid ------------------
,first_bill as (
        SELECT DATE_TRUNC('month',date(first_inst_dt)) as Month,act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill
        FROM
            (select act_acct_cd,dt,
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
            FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
            FROM "db-analytics-prod"."tbl_fixed_cwc"
            WHERE org_cntry = 'Jamaica'
            AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard')
            AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
            and act_acct_cd in (select act_acct_cd from sales)
            AND date(dt) between (Date_trunc('Month', date(act_acct_inst_dt)) - interval '6' month) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '2' month)
            AND oldest_unpaid_bill_dt <> '19000101')
        group by 1,act_acct_cd
        )

,max_overdue_first_bill as (
        select DATE_TRUNC('month',date(min(first_inst_dt))) as Month, act_acct_cd, 
        min(date(date_parse(first_oldest_unpaid_bill_dt, '%Y%m%d'))) as first_oldest_unpaid_bill_dt,
        min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
        concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
        max(fi_outst_age) as max_fi_outst_age, 
        max(date(dt)) as max_dt,
        max(case when pd_mix_cd is null then 0 else cast(replace(pd_mix_cd,'P','') as int) end) as RGUs,
        case when max(cast(fi_outst_age as int))>= 90 then 1 else 0 end as never_paid_flg,
        case when max(cast(fi_outst_age as int))>=36 then 1 else 0 end as soft_dx_flg,
        case when (min(date(date_parse(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval  '90'  day) < current_date then 1 else 0 end as neverpaid_window,
        case when (min(date(date_parse(first_oldest_unpaid_bill_dt, '%Y%m%d'))) + interval  '36'  day) < current_date then 1 else 0 end as softdx_window
        FROM
            (select act_acct_cd,
            FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt,
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
            FIRST_VALUE(date(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt,
            fi_outst_age, dt, pd_mix_cd
            FROM "db-analytics-prod"."tbl_fixed_cwc"
            WHERE 
            org_cntry = 'Jamaica'
            AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard')
            AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
            and concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill)
            AND date(dt) between Date_trunc('Month', date(act_acct_inst_dt)) and (Date_trunc('Month', date(act_acct_inst_dt)) + interval '5' month)
            )
        group by act_acct_cd
)

,so_inst_date_search as (
select account_id, MIN(DATE(CAST(completed_date AS TIMESTAMP))) AS completed_so_dt
            from "db-stage-dev"."so_hdr_cwc"
            WHERE order_status = 'COMPLETED'
            AND order_type = 'INSTALLATION'
            AND (network_type NOT IN ('LTE','MOBILE') OR network_type IS NULL) 
            --and DATE_TRUNC('month',DATE(CAST(completed_date AS TIMESTAMP))) BETWEEN date(dt) AND date(dt) + interval '1' month
            AND CAST(account_id AS VARCHAR) IN (select act_acct_cd from max_overdue_first_bill) group by 1 )
        
,final_inst_dt AS (
    SELECT *,Soft_Dx_flg as SoftDx_Flag, never_paid_flg as NeverPaid_Flag, CASE WHEN completed_so_dt > first_inst_dt THEN completed_so_dt ELSE first_inst_dt END AS first_inst_dt_final
    FROM max_overdue_first_bill m
    LEFT JOIN so_inst_date_search s
        ON m.act_acct_cd = CAST(s.account_id AS VARCHAR) and m.month = date_trunc('Month', s.completed_so_dt)   )
        
,final as(
select *,
case when DATE_ADD('day',90, first_oldest_unpaid_bill_dt) < current_date then 1 else 0 end as never_paid_window_completed,
DATE_ADD('day',90, first_oldest_unpaid_bill_dt) as threshold_never_paid_date,
case when DATE_ADD('day',36, first_oldest_unpaid_bill_dt) < current_date then 1 else 0 end as soft_dx_window_completed,
DATE_ADD('day',36, first_oldest_unpaid_bill_dt) as threshold_soft_dx_date,
current_date as current_date_analysis,
DATE_TRUNC('month', DATE_ADD('day',90, first_oldest_unpaid_bill_dt)) AS never_paid_month,
DATE_TRUNC('month',DATE_ADD('day',36, first_oldest_unpaid_bill_dt)) AS soft_dx_month
from final_inst_dt
)

,final_w_fmc as (
select f.*, a.act_acct_cd,
a.first_oldest_unpaid_bill_dt,
a.first_inst_dt,
a.first_act_cust_strt_dt,
a.act_first_bill,
a.max_fi_outst_age,
a.max_dt,
a.RGUs,
a.NeverPaid_Flag,
a.SoftDx_Flag,
a.neverpaid_window,
a.softdx_window,
a.completed_so_dt,
a.first_inst_dt_final,
a.never_paid_window_completed,
a.threshold_never_paid_date,
a.soft_dx_window_completed,
a.threshold_soft_dx_date,
a.current_date_analysis,
a.never_paid_month,
a.soft_dx_month, CASE WHEN a.act_acct_cd is not null then 1 else 0 end as monthsale_flag

FROM FMC_Table f left join final a
    ON f.fixed_account = a.act_acct_cd and f.month = a.month

)

--select COUNT(DISTINCT act_acct_cd)
--FROM final_w_fmc --final
--WHERE DATE_TRUNC('month',first_inst_dt_final) = date('2022-04-01')
--select * from final_w_fmc limit 10
-------------------------- Late Installations -----------------------

,install_data as (
select 
    date_trunc('Month',cast (act_cust_strt_dt as date)) as Sales_Month,
    date_trunc('Month',cast (act_acct_inst_dt as date)) as Install_Month,
    act_acct_cd,act_cust_strt_dt, act_acct_inst_dt,
    case when length(act_acct_cd)=8 then 'CERILLION' ELSE 'LIBERATE' end as CRM
from 
    (
        select * from "db-analytics-prod"."tbl_fixed_cwc"
    )
where (act_acct_cd) in (select (act_acct_cd) from sales) 
)

,install_summary AS (
    SELECT 
        Sales_Month, 
        Install_Month, act_acct_cd,max(CRM) as CRM, min( DATE(act_cust_strt_dt))  as act_cust_strt_dt, min( DATE(act_acct_inst_dt)) as act_acct_inst_dt,
        case when date_diff('day',min( DATE(act_cust_strt_dt)),min( DATE(act_acct_inst_dt)))>=6 then 1 else 0 end as long_install_flag
    from install_data
    GROUP BY act_acct_cd
    , sales_month
    , install_month
)

,Late_install_flag AS(
    SELECT f.*, sales_month, install_month, long_install_flag
    FROM final_w_fmc f LEFT JOIN Install_Summary i 
    on f.fixed_account = i.act_acct_cd AND f.month
= i.Install_Month
)

------------------MRC Changes------------------------------------------------

,previous_month_base as (
select Month, act_acct_cd,first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
from(select act_acct_cd, date_trunc('MONTH', DATE(dt)) as Month, 
first_value(fi_outst_age) over(partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt) as first_due_record,
first_value(fi_outst_age) over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_due_record,
first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as last_MRC,
first_value(bundle_code) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_bundle,
first_value(pd_bb_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_bb_prod_nm,
first_value(pd_vo_prod_nm) IGNORE NULLS over(partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as pd_vo_prod_nm,
first_value(pd_tv_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)), act_acct_cd order by dt desc) as pd_tv_prod_nm 
from "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry='Jamaica'
AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
GROUP BY Month, act_acct_cd, first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
HAVING (cast(first_due_record as int) <=90  or first_due_record is null) AND (cast(last_due_record as int) <=90 or last_due_record is null )
)

,actual_month_base as (
select Month, act_acct_cd,first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
from( select act_acct_cd, --dt, 
date_trunc('MONTH', DATE(dt)) as Month,
first_value(fi_outst_age) over(partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt) as first_due_record,
first_value(fi_outst_age) over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_due_record,
first_value(cast(fi_tot_mrc_amt as double)) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_MRC,
first_value(bundle_code) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as last_bundle,
first_value(pd_bb_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_bb_prod_nm,
first_value(pd_vo_prod_nm) IGNORE NULLS over(partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_vo_prod_nm,
first_value(pd_tv_prod_nm) IGNORE NULLS over (partition by date_trunc('MONTH', DATE(dt)),act_acct_cd order by dt desc) as pd_tv_prod_nm 
from "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry='Jamaica' 
AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
)
GROUP BY Month, act_acct_cd, first_due_record, last_due_record, last_MRC, last_bundle, pd_bb_prod_nm, pd_vo_prod_nm, pd_tv_prod_nm
HAVING (cast(first_due_record as int) <=90  or first_due_record is null) AND (cast(last_due_record as int) <=90 or last_due_record is null )
)

,joint_bases as (
select a.Month as Real_month, b.Month, a.act_acct_cd, case when  b.last_MRC= 0 then null else a.last_MRC / b.last_MRC end as MRC_growth, a.last_bundle as actual_bundle, b.last_bundle as previous_bundle,
concat(a.pd_bb_prod_nm,a.pd_vo_prod_nm,a.pd_tv_prod_nm) actual_plans, concat(b.pd_bb_prod_nm,b.pd_vo_prod_nm,b.pd_tv_prod_nm) previous_plans
from actual_month_base a
left join previous_month_base b
on a.act_acct_cd = b.act_acct_cd and a.Month = DATE_ADD('MONTH', 1, b.Month)
order by MRC_growth desc
)

,MRCFlag_summary as(
select Real_Month, act_acct_cd,
case when MRC_growth > 1.05 AND (actual_bundle=previous_bundle or actual_plans=previous_plans) then 1 else 0 end as increase_flag,
case when (actual_bundle=previous_bundle or actual_plans=previous_plans) then 1 else 0 end as no_plan_change_flag
from joint_bases
)

,FullTable_MRCFlag AS(
    SELECT f.*, m.increase_flag, m.no_plan_change_flag
    FROM Late_install_flag f LEFT JOIN MRCFlag_Summary m 
    ON f.fixed_account = m.act_acct_cd and f.month
= m.Real_Month
)

,MRCFlag_Test AS(

  SELECT m.*
    FROM Late_install_flag f RIGHT JOIN MRCFlag_Summary m 
    ON f.final_account = m.act_acct_cd and f.month
= m.Real_Month
    WHERE f.final_account is null
)

------------------------------ Mounting Bills-------------------------------------
,mountingbills_initial_table as (
select 
    date_trunc('Month',cast (dt as date)) as Month, act_acct_cd, 
    case when fi_outst_age is null then -1 else cast(fi_outst_age as double) end as fi_outst_age,
    case when fi_outst_age='60' --AND cast(fi_bill_amt_m0 as double) is not null and cast(fi_bill_amt_m0 as double) >0 
    then 1 else 0 end as day_60,
    first_value(case when fi_outst_age is null then -1 else cast(fi_outst_age as double) end) IGNORE NULLS over(partition by date_trunc('Month',cast (dt as date)), act_acct_cd order by dt desc) as last_overdue_record,
    first_value(case when fi_outst_age is null then -1 else cast(fi_outst_age as double) end) IGNORE NULLS over(partition by date_trunc('Month',cast (dt as date)), act_acct_cd order by dt) as first_overdue_record
from   "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry = 'Jamaica' AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and cast(dt as date) between date_trunc('MONTH', cast(dt as date)) and date_add('MONTH', 1, date_trunc('MONTH', cast(dt as date)))
)

,FinalMountingBills as (
select Month, act_acct_cd,
max(fi_outst_age) as max_overdue,
max(day_60) as day_60,
max(last_overdue_record) as last_overdue_record,
max(first_overdue_record) as first_overdue_record
from mountingbills_initial_table
GROUP BY Month, act_acct_cd
having max(last_overdue_record) <=90 AND  max(first_overdue_record) <=90 
)

,FullTable_MountBills_Flag AS(
SELECT f.*, day_60 as mountingbill_flag
FROM FullTable_MRCFlag f LEFT JOIN FinalMountingBills b
ON f.fixed_account = b.act_acct_cd AND f.month= b.Month
)

------------------------- Early Tickets ---------------------------------------

,clean_interactions_base AS(
select  * from "db-stage-dev"."interactions_cwc" where partition_0 = 'cerillion' 
union all
select  B.*
        from (select * from "db-stage-dev"."interactions_cwc" where partition_0 = 'cerillion') A
        right join 
             (select * from "db-stage-dev"."interactions_cwc" where partition_0 = 'acut') B
        on cast(A.interaction_start_time as date) = cast(B.interaction_start_time as date) and REGEXP_REPLACE(A.account_id,'[^0-9 ]','') = REGEXP_REPLACE(B.account_id,'[^0-9 ]','')
        where cast(A.interaction_start_time as date) is null and REGEXP_REPLACE(A.account_id,'[^0-9 ]','') is null
)

,initial_table_tickets as (
SELECT date(date_trunc('MONTH', interaction_start_time)) as Ticket_Month, interaction_id, account_id_2, min(date(interaction_start_time)) as interaction_start_time
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from clean_interactions_base
where lower(org_cntry) like '%jam%') --AND date(interaction_start_time)  between
--date('2022-02-01') AND date('2022-02-28')) 
GROUP BY 1,Interaction_id, account_id_2
)

,installations as (
select 
    date_trunc('Month', min(date(act_cust_strt_dt))) as Sales_Month,
    act_acct_cd, min(date(act_cust_strt_dt)) as act_cust_strt_dt,
    min(date(act_acct_inst_dt)) as act_acct_inst_dt,
    date_trunc('MONTH', min(date(act_acct_inst_dt))) as Inst_Month
from   "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry = 'Jamaica' AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and date(act_cust_strt_dt) between date_trunc('Month',cast (act_cust_strt_dt as date)) and date_add('DAY',90, date_trunc('Month',cast (act_cust_strt_dt as date)))
GROUP BY act_acct_cd
)

,joint_bases_et as (
select t.*, i.sales_month, i.act_cust_strt_dt,
i.inst_month, i.act_acct_inst_dt
from initial_table_tickets t
left join installations i
on t.account_id_2 = i.act_acct_cd
)

,account_summary_tickets as (
select account_id_2 as ACCOUNT_ID, max(case when date_diff('week',act_acct_inst_dt,interaction_start_time)<=7 then 1 else 0 end ) as early_tickets, Sales_Month,
Inst_Month,
date_trunc('MONTH', interaction_start_time) AS Ticket_Month
from joint_bases_et
GROUP by Sales_Month, account_id_2, Inst_Month,5
)

,FullTable_EarlyTickets as(
select f.*, early_tickets as EarlyTicket_Flag, ticket_month
from FullTable_MountBills_Flag f LEFT JOIN account_summary_tickets  b
ON f.fixed_account = b.Account_ID AND f.month= b.Ticket_Month

)

,saleschannel_SO AS(
 SELECT date_trunc('Month', min(DATE(order_start_date))) as Month, 
 date_trunc('Month', min(date(completed_date))) as Inst_Month, max(channel_type) as sales_channel, account_id
FROM "db-stage-dev"."so_hdr_cwc"
WHERE  org_cntry = 'Jamaica' AND network_type NOT IN ('LTE','MOBILE')
        AND order_status = 'COMPLETED'
        AND account_type = 'Residential'
        AND order_type = 'INSTALLATION'
        AND cease_reason_group IS NULL
GROUP BY account_id
)

,FullTable_SalesChannel AS(
 
 SELECT  f.*, 
  ------ Modificar clasificación de canal en caso de que aparezca Affiliate sales ----
 CASE WHEN sales_channel in ( 'DEALERS', 'RETAIL') THEN 'RETAIL'
 WHEN sales_channel in ('ECOMMERCE', 'VIRTUAL SALES') THEN 'DIGITAL'
 WHEN sales_channel in ('D2D') THEN sales_channel
 WHEN sales_channel in ('TELESALES' ) THEN 'TELESALES_INBOUND'
 WHEN sales_channel in ('CUSTOMER CARE', 'TECH SUPPORT', 'CALL CENTER', 'RETENTION DEPARMENT', 'ORDER MANAGEMENT', 'PROCESSING', 'DISPATCH') THEN 'TELESALES_OUTBOUND'
 END AS Sales_channel,
 CASE when s.account_id is not null then 'Match'
 else 'No match' end as saleschannel_flag
 FROM FullTable_EarlyTickets f LEFT JOIN saleschannel_SO s
 ON f.fixed_account = cast(s.account_id as varchar) and f.install_month = s.inst_month

)

,FullTable_KPIsFlags AS(
Select *, case when Monthsale_flag = 1 then concat(cast(Monthsale_flag as varchar), fixed_account) else NULL end as F_SalesFlag, 
    case when SoftDx_flag = 1 then concat(cast(SoftDx_Flag as varchar), fixed_account) else NULL end as F_SoftDxFlag, 
    case when NeverPaid_flag = 1 then concat(cast(NeverPaid_Flag as varchar), Fixed_Account) else NULL end as F_NeverPaidFlag, 
    case when Long_install_flag = 1 then concat(cast(long_install_flag as varchar), Fixed_account) else NULL end as F_LongInstallFlag,
    case when Increase_flag = 1 then concat(cast(increase_flag as varchar), Fixed_Account) else NULL end as F_MRCIncreases, 
    case when No_plan_change_flag = 1 then concat(cast(no_plan_change_flag as varchar),fixed_account) else NULL end as F_NoPlanChangeFlag,
    case when Mountingbill_flag = 1 then concat(cast(mountingbill_flag as varchar), fixed_account) else NULL end as F_MountingBillFlag,
   case when EarlyTicket_Flag = 1 then concat(cast(earlyticket_flag as varchar), fixed_account) else NULL end as F_EarlyTicketFlag
From FullTable_SalesChannel )

,Results_Table AS(
select Month as ActiveBase_Month
, E_Final_Tech_Flag, E_FMC_Segment, E_FMCType, E_FinalTenureSegment, 
count(distinct fixed_account) as activebase
, sales_channel
, sum(monthsale_flag) as Sales, sum(SoftDx_Flag) as Soft_Dx, sum (NeverPaid_Flag) as NeverPaid, sum(long_install_flag) as Long_installs, sum (increase_flag) as MRC_Increases, sum (no_plan_change_flag) as NoPlan_Changes, sum(mountingbill_flag) as MountingBills, sum(earlyticket_flag) as EarlyTickets, 
Sales_Month, Install_Month, Ticket_Month
, count(distinct F_SalesFlag) Unique_Sales, count(distinct F_SoftDxFlag) Unique_SoftDx,
    count(distinct F_NeverPaidFlag) Unique_NeverPaid,count(distinct F_LongInstallFlag) Unique_LongInstall,
    count(distinct F_MRCIncreases) Unique_MRCIncrease,count(distinct F_NoPlanChangeFlag) Unique_NoPlanChanges,
    count(distinct F_MountingBillFlag) Unique_Mountingbills,
   count (distinct F_EarlyTicketFlag) Unique_EarlyTickets
from FullTable_KPIsFlags
where finalchurnflag <> 'Fixed Churner' and waterfall_flag <> 'Downsell-Fixed Customer Gap' and waterfall_flag <> 'Fixed Base Exception' and mainmovement <> '6.Null last day' and waterfall_flag <> 'Churn Exception'
and month = date(dt)
Group by 1,2,3,4,5,7,16,17,18
Order by 1,2,3,4,5,7,16,17,18
)

SELECT *
From Results_Table
