with

Cuentas_A_Evaluar as (
select distinct mobile_account
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month = date('2022-08-01') --and Mobile_RejoinerMonth=0 
and drc=1 and mobile_activebom =1 --and mobile_activeeom =1 
group by 1
order by 1
)

,MobileUsefulFields AS(
SELECT DATE(dt) AS DT, DATE_TRUNC('MONTH', DATE(dt)) AS MobileMonth
,ACCOUNTNO AS MobileAccount
, CAST(SERVICENO AS INT) AS PhoneNumber
,MAX(CAST(DATE_PARSE(STARTDATE_ACCOUNTNO, '%Y.%m.%d %T') AS DATE)) AS MaxStart
,ACCOUNTNAME AS Mob_AccountName,NUMERO_IDENTIFICACION as Mobile_Id
,CAST(TOTAL_MRC_D AS DECIMAL) AS Mobile_MRC
,CAST(DATE_PARSE(INV_EXP_DT, '%Y.%m.%d %T') AS DATE) AS MobilePay_Dt,
ACCOUNT_STATUS
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE "biz_unit_d"='B2C' AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>'nan' 
 --AND date(dt) between (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY - interval '3' MONTH) AND  (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY + interval '3' MONTH)
GROUP BY DT,2,3,
4,ACCOUNTNAME,7,8,9,10
)

,join_cuentas as (
select A.mobile_account as cuentas_a_evaluar, B.* 
from Cuentas_A_Evaluar A left join MobileUsefulFields B on A.mobile_account = B.MobileAccount
)

select distinct DT, ACCOUNT_STATUS,count (distinct MobileAccount), count(distinct cuentas_a_evaluar)
from join_cuentas
where year(dt) >= 2022 --and ACCOUNT_STATUS='GROSS_ADDS'
group by 1,2
order by 1,2
