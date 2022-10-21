WITH 

Cuentas_Evaluar as (
SELECT distinct mobile_account
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-07-01') and mobile_activebom =1
)

,FMC_Julio as (
select *, mobile_activeeom as EOM_Active_julio, mobile_activebom as BOM_Active_julio
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-07-01') 
)

,FMC_Junio as (
select  *, mobile_activeeom as EOM_Active_junio, mobile_activebom as BOM_Active_junio
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-06-01') 
)

,join_estatus as (
select 
C.*, BOM_Active_julio, EOM_Active_julio, 
case when BOM_Active_junio=1 and EOM_Active_junio=1 and BOM_Active_julio = 1 and EOM_Active_julio =1 then 'Clean' 
        when BOM_Active_junio=1 and EOM_Active_junio=1 and BOM_Active_julio = 1 and EOM_Active_julio =0 then 'Churner July'
        when BOM_Active_junio=0 and EOM_Active_junio=1 and BOM_Active_julio = 1 and EOM_Active_julio =1 then 'Adds June'
        when BOM_Active_junio=1 and EOM_Active_junio=0 and BOM_Active_julio = 1 and EOM_Active_julio =1 then 'Damage 1011'
        when BOM_Active_junio=1 and EOM_Active_junio=1 and BOM_Active_julio = 0 and EOM_Active_julio =1 then 'Damage 1101'
        when BOM_Active_junio=1 and EOM_Active_junio=0 and BOM_Active_julio = 1 and EOM_Active_julio =0 then 'Damage 1010'
        when BOM_Active_junio=0 and EOM_Active_junio=0 and BOM_Active_julio = 1 and EOM_Active_julio =1 then 'Damage 0011'
        when BOM_Active_junio is null and EOM_Active_junio is null and BOM_Active_julio = 1 and EOM_Active_julio =0 then 'Damage nn10'
        when BOM_Active_junio is null and EOM_Active_junio is null and BOM_Active_julio = 1 and EOM_Active_julio =1 then 'Damage nn11'
        when BOM_Active_junio =0 and EOM_Active_junio =1 and BOM_Active_julio = 1 and EOM_Active_julio =0 then 'Case 0110'
        else null end as bandera1
from
(select A.*, BOM_Active_junio, EOM_Active_junio
from Cuentas_Evaluar A left join FMC_Junio B on A.mobile_account = B.mobile_account) C left join FMC_Julio D on C.mobile_account = D.mobile_account
)

,Sample_Damage1011 as (
select distinct mobile_account
--distinct bandera1, BOM_Active_junio, EOM_Active_junio, BOM_Active_julio, EOM_Active_julio, count (distinct mobile_account) 
from join_estatus 
where bandera1='Damage 1011'
--group by 1,2,3,4,5
--order by 1
)

,MobileUsefulFields AS(
SELECT DATE(dt) AS DT, DATE_TRUNC('MONTH', DATE(dt)) AS MobileMonth
,ACCOUNTNO AS MobileAccount
, CAST(SERVICENO AS INT) AS PhoneNumber
,MAX(CAST(DATE_PARSE(STARTDATE_ACCOUNTNO, '%Y.%m.%d %T') AS DATE)) AS MaxStart
,ACCOUNTNAME AS Mob_AccountName,NUMERO_IDENTIFICACION as Mobile_Id
,CAST(TOTAL_MRC_D AS DECIMAL) AS Mobile_MRC
,cast(date_parse((case when INV_EXP_DT = '0' then null else INV_EXP_DT end), '%Y.%m.%d %T') as date) AS MobilePay_Dt
,ACCOUNT_STATUS
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE "biz_unit_d"='B2C' AND ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND INV_EXP_DT<>'nan' 
AND date(dt) between DATE('2022-05-15') and date('2022-08-15')
GROUP BY DT,2,3,4,ACCOUNTNAME,7,8,9,10
)

,Sample_DNA as (
select B.* 
from Sample_Damage1011 A left join MobileUsefulFields B on A.mobile_account=B.MobileAccount
)


,NumberRGUsPerUser AS(
SELECT DISTINCT MobileMonth,dt,MobileAccount,count(distinct PHONENUMBER) AS NumRGUs
FROM Sample_DNA
GROUP BY MobileMonth,dt,MobileAccount
)

,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS Month, MobileAccount, Round(avg(Mobile_MRC),0) AS AvgMRC_Mobile
  FROM Sample_DNA 
  WHERE Mobile_MRC IS NOT NULL AND Mobile_MRC <> 0
  GROUP BY 1, MobileAccount
)

,MobileActive_BOM AS(
SELECT m.DT AS B_Date, DATE_TRUNC('MONTH', DATE_ADD('MONTH', 1, DATE(m.dt))) AS MobileMonth,
m.MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart
, Mob_AccountName as B_Mob_Acc_Name, Mobile_Id as B_Mobile_ID
, round(Mobile_MRC,0) as B_MobileMRC
, NumRGUs AS B_MobileRGUs, round(AvgMRC_Mobile,0) as B_AvgMobileMRC
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
WHEN DATE_DIFF('DAY',MaxStart, m.dt)>180 AND DATE_DIFF('DAY',MaxStart, m.dt)<= 360 THEN 'Mid-Tenure'
      WHEN DATE_DIFF('DAY',MaxStart, m.dt)>360 THEN 'Late-Tenure' END AS B_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt)= DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
)

,MobileActive_EOM AS(
SELECT m.DT AS E_Date, DATE_TRUNC('MONTH', DATE(m.dt)) AS MobileMonth,
m.MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart
, Mob_AccountName as E_Mob_Acc_Name, Mobile_Id as E_Mobile_ID
, round(Mobile_MRC,0) as E_MobileMRC
, NumRGUs AS E_MobileRGUs, round(AvgMRC_Mobile,0) as E_AvgMobileMRC
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
WHEN DATE_DIFF('DAY',MaxStart, m.dt)>180 AND DATE_DIFF('DAY',MaxStart, m.dt)<=360 THEN 'Mid-Tenure'
      WHEN DATE_DIFF('DAY', MaxStart, m.dt)>360 THEN 'Late-Tenure' END AS E_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt) = DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
)

 ,MobileCustomerStatus AS(
  SELECT DISTINCT
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN b.MobileMonth
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN e.MobileMonth
  END AS Mobile_Month,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN mobileBOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN mobileEOM
  END AS Mobile_Account,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN Phone_BOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN Phone_EOM
  END AS PhoneNumber,
  CASE WHEN mobileBOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
  CASE WHEN mobileEOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
  b.*, e.* 
  FROM MobileActive_BOM as b FULL OUTER JOIN MobileActive_EOM as e on b.MobileBOM = e.MobileEOM and b.MobileMonth = e.MobileMonth
)

select * from MobileCustomerStatus where Mobile_Account='1813072'
