---------------------------------------------------------------------------------------------
------------------------- BREECH CASES PARAMETRIZADO - V1 -----------------------------------
---------------------------------------------------------------------------------------------

WITH

parameters as(
select 
date_trunc('month',date('2022-10-01')) as input_month
)

,Service_Orders AS(
SELECT  *
        ,date_trunc('Month', DATE(order_start_date)) as month
        ,DATE(order_start_date) AS StartDate
        ,DATE(completed_date) AS EndDate
        ,date_diff ('DAY',DATE(order_start_date),DATE(completed_date)) AS Installation_lapse
FROM "db-stage-dev"."so_hdr_cwp"
WHERE order_type = 'INSTALLATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
        AND DATE_TRUNC('MONTH',CAST(order_start_date AS DATE)) = (SELECT input_month FROM PARAMETERS)
)

,previous_months_dna as (
SELECT  distinct DATE_TRUNC('MONTH',CAST(DT AS DATE)) AS month
        ,act_acct_cd
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        --AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
        AND date(dt) between ((SELECT input_month FROM PARAMETERS) - interval '4' month) and ((SELECT input_month FROM PARAMETERS) - interval '1' month)
GROUP BY 1,2
ORDER BY 1,2
)

--select distinct month from previous_months_dna

,current_month_dna as (
SELECT  distinct DATE_TRUNC('MONTH',CAST(DT AS DATE)) AS month
        ,act_acct_cd
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        --AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
        AND DATE_TRUNC('MONTH',CAST(DT AS DATE)) = (SELECT input_month FROM PARAMETERS)
GROUP BY 1,2
ORDER BY 1,2
)

,bridge as (
SELECT  DISTINCT month
        ,account_id
        ,order_id
        ,case when Installation_lapse >5 then '6+' else cast(installation_lapse as varchar) end as installation_lapse
        ,CASE WHEN cast(account_id AS VARCHAR) in (select distinct cast(act_acct_cd AS VARCHAR) from current_month_dna) and cast(account_id AS VARCHAR) not in (select distinct cast(act_acct_cd AS VARCHAR) from previous_months_dna) then 1 else 0 end as new_customer 
FROM Service_Orders 
GROUP BY 1,2,3,4,5
)

,denominador as(
select distinct month, count(distinct account_id) as denom
from bridge
where new_customer =1
group by 1
order by 1
)

,denominador2 as(
select distinct month, count(distinct act_acct_cd) as denom
from (SELECT *,
CASE WHEN act_acct_cd NOT IN (select distinct act_acct_cd from previous_months_dna) THEN 1 ELSE 0 END AS new_customer
from current_month_dna)
where new_customer =1
group by 1
order by 1
)

select  distinct a.month
        ,a.installation_lapse
        ,count(distinct a.account_id) as cuentas
        ,b.denom
        ,round((cast(count(distinct A.account_id) as double)/cast(b.denom as double))*100,4) as kpi_meas_cuentas
from bridge A left join denominador b on a.month=b.month
where installation_lapse = '6+'
group by 1,2,4
order by 1,2,4
