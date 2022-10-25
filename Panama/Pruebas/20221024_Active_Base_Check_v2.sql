WITH

Cuentas_Evaluar as (
SELECT distinct mobile_account
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-07-01') and mobile_activebom =1
)

,FMC_Julio as (
select *, mobile_activeeom as EOM_Active_julio, mobile_activebom as BOM_Active_julio, MobileChurnerType as ChurnerTypeJulio
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-07-01') 
)

,FMC_Junio as (
select  *, mobile_activeeom as EOM_Active_junio, mobile_activebom as BOM_Active_junio, MobileChurnerType as ChurnerTypeJunio
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" 
where month=date(dt) and month = date('2022-06-01') 
)

,join_estatus as (
select 
C.*, BOM_Active_julio, EOM_Active_julio, ChurnerTypeJulio,
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
(select A.*, BOM_Active_junio, EOM_Active_junio, ChurnerTypeJunio
from Cuentas_Evaluar A left join FMC_Junio B on A.mobile_account = B.mobile_account) C left join FMC_Julio D on C.mobile_account = D.mobile_account
)

--,Sample_Damage1011 as (
select 
--distinct mobile_account
distinct bandera1, BOM_Active_junio, EOM_Active_junio, BOM_Active_julio, EOM_Active_julio, ChurnerTypeJunio,count (distinct mobile_account) 
from join_estatus 
--where bandera1='Damage 1011'
group by 1,2,3,4,5,6
order by 1
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
--AND date(dt) between (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY - interval '3' MONTH) AND  (DATE('2022-06-01') + interval '1' MONTH - interval '1' DAY + interval '3' MONTH)
GROUP BY DT,2,3,4,ACCOUNTNAME,7,8,9,10
)

,Cruce_DRC as (
select distinct dt, count(distinct accountno) from Sample_Damage1011 A left join "lla_cco_int_ext_prod"."cwp_mov_ext_derecognition" B on A.mobile_account=CAST(b.ACCOUNTNO AS VARCHAR(50)) 
group by 1
order by 1
)

select distinct DT, count(distinct MobileAccount) from Sample_Damage1011 A left join MobileUsefulFields B on A.mobile_account=B.MobileAccount
where DT in (date('2022-04-30'),date('2022-05-31'),date('2022-06-30'),date('2022-07-31'),date('2022-08-31'))
group by 1
order by 1
