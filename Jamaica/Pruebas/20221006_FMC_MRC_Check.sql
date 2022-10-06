with

Cuentas_A_Evaluar as (
SELECT distinct month, E_FMC_Status, E_FMCType, Mobile_ActiveBOM, Mobile_ActiveEOM, Final_Account, substring(Final_Account,position('-' in Final_Account)+1,20) as mobile_account
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
where E_FMCType = 'Fixed 2P' and E_FMC_Status = 'Soft/Hard FMC' and Mobile_ActiveEOM = 0 and Mobile_ActiveBOM =1 and month >= date ('2022-09-01')
group by 1,2,3,4,5,6
order by 3,2,4,5,1
)

, DNA_MobileFields as (
   SELECT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, account_id,dt,phone_no, 
   CASE WHEN IS_NAN (cast(total_mrc_mo as double)) THEN 0
   WHEN NOT IS_NAN  (cast(total_mrc_mo as double)) THEN round(cast(total_mrc_mo as double),0)
   END AS total_mrc_mo, 
   avg(cast(total_mrc_mo as double)) over (partition by account_id, DATE_TRUNC ('MONTH',DATE(dt)) order by dt) as average_mrc,
   DATE_DIFF('DAY',  date(first_value(account_creation_date) over(partition by account_id order by dt desc)),  date(first_value(dt) over(partition by account_id order by dt desc))) as MaxTenureDays,
   --first_value(account_creation_date) over(partition by account_id order by dt desc) AS Mobile_MaxStart,
   cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date) as oldest_unpaid_bill_dt_adj,
   date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as Fi_outst_age
   FROM "db-analytics-prod"."tbl_postpaid_cwc" 
   WHERE org_id = '338' AND account_type ='Residential'
   AND account_status NOT IN('Ceased','Closed','Recommended for cease')
   -- Variabilizar mes de reporte
   AND date(dt) between (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY)
)

 ,AverageMRC_Mobile AS(
  SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, account_id, phone_no
  , round(avg(total_mrc_mo),0) AS AvgMRC_Mobile
  FROM DNA_MobileFields
  WHERE total_mrc_mo IS NOT NULL AND total_mrc_mo <> 0 AND NOT IS_NAN(total_mrc_mo)
  GROUP BY 1, account_id,phone_no
  )



,JoinTable as(
select A.*, C.AvgMRC_Mobile
from  DNA_MobileFields A right join Cuentas_A_Evaluar B on A.account_id=B.mobile_account
left join AverageMRC_Mobile C on A.account_id =C.account_id and C.Month = A.Month 
where date(A.dt)>=date('2022-08-01') 
)




Select distinct dt, account_id, AvgMRC_Mobile, total_mrc_mo,Fi_outst_age,MaxTenureDays, bandera--distinct dt, count(distinct account_id) 
--distinct month, Bandera, count (distinct account_id) as count
--distinct month, dt, Bandera, account_id
from(
select *, case when AvgMRC_Mobile <> 0 or AvgMRC_Mobile is not null then 'OK MRC' else 'MRC null or 0 all month' end as bandera
from JoinTable
)
--group by 1 
--where date(dt) = date('2022-09-30') 
group by 2,1,3,4,5,6,7
order by 2,1,3,4



--select dt, sum(CASE WHEN IS_NAN (cast(total_mrc_mo as double)) THEN 0
--   WHEN NOT IS_NAN  (cast(total_mrc_mo as double)) THEN round(cast(total_mrc_mo as double),0)
--   END ) AS total_mrc_mo FROM "db-analytics-prod"."tbl_postpaid_cwc" where date(dt) >= date('2022-04-01') group by 1 order by 1
