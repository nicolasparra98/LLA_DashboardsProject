WITH

NEW_CUSTOMERS_original as (
Select 
act_acct_cd,date(dt) as dt, DATE_TRUNC('MONTH',CAST(dt AS DATE)) AS month_load,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu, max(act_acct_inst_dt) as act_acct_inst_dt ,max(act_cust_strt_dt) as act_cust_strt_dt,  DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)) as Installation_lapse, 1 as NEW_CUSTOMER,
pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
GROUP BY act_acct_cd, 2, DATE_TRUNC('MONTH',CAST(dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT),1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
)


,NEW_CUSTOMERS_updated as (
Select  act_acct_cd,
        date(dt) as dt, 
        DATE_TRUNC('MONTH',CAST(dt AS DATE)) AS month_load,
        DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,
        CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu, 
        --max(act_acct_inst_dt) as act_acct_inst_dt ,
        --max(act_cust_strt_dt) as act_cust_strt_dt, 
        first_value(act_acct_inst_dt) OVER (PARTITION BY act_acct_cd, DATE_TRUNC('MONTH',CAST(dt AS DATE)) ORDER BY date(dt)) as act_acct_inst_dt,
        first_value(act_cust_strt_dt) OVER (PARTITION BY act_acct_cd, DATE_TRUNC('MONTH',CAST(dt AS DATE)) ORDER BY date(dt)) as act_cust_strt_dt,
        DATE_DIFF ('DAY',CAST (first_value(act_cust_strt_dt) OVER (PARTITION BY act_acct_cd, DATE_TRUNC('MONTH',CAST(dt AS DATE)) ORDER BY date(dt)) AS DATE),CAST (first_value(act_acct_inst_dt) OVER (PARTITION BY act_acct_cd, DATE_TRUNC('MONTH',CAST(dt AS DATE)) ORDER BY date(dt)) AS DATE)) as Installation_lapse,
        1 as NEW_CUSTOMER,
        pd_bb_accs_media,
        pd_tv_accs_media,
        pd_vo_accs_media
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
GROUP BY act_acct_cd, 2, DATE_TRUNC('MONTH',CAST(dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT),1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media,act_acct_inst_dt,act_cust_strt_dt
)

,denominador as (
select distinct month_load, count(distinct act_acct_cd) as denominador
from new_customers_original
group by 1
)

,solucion as(
select distinct a.month_load, case when a.Installation_lapse >5 then '6+' else cast(a.installation_lapse as varchar) end as  installation_lapse, count(distinct a.act_acct_cd) as cuentas, b.denominador
from new_customers_updated a left join denominador b on a.month_load=b.month_load
where a.month_load >= date('2022-01-01')
group by 1,2,4
order by 1,2,4
)

--select * from new_customers
--where act_acct_cd='321083150000'

select distinct month_load, installation_lapse, cuentas, denominador, round(cast(cuentas as double)/cast(denominador as double)*100, 4)
from solucion
having installation_lapse ='6+'
