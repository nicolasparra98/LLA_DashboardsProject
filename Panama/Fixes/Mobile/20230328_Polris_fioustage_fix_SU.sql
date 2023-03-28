-- Sprint 2 Mobile

WITH
Convergente AS(
SELECT DISTINCT *,DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d')) as Mes

FROM "lla_cco_int_ext_dev"."cwp_con_ext_fmc"

WHERE telefonia='Pospago' AND "unidad de negocio"='1. B2C' 
 AND DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-11-01') or DATE_TRUNC('MONTH', DATE_PARSE(CAST(Date AS VARCHAR(10)), '%Y%m%d'))=DATE('2022-10-01')
)

,MobileUsefulFields AS(
SELECT DATE(a.dt) AS DT, DATE_TRUNC('MONTH', DATE(a.dt)) AS MobileMonth
,a.ACCOUNTNO AS MobileAccount
, CAST(a.SERVICENO AS INT) AS PhoneNumber
,MAX(CAST(DATE_PARSE(a.STARTDATE_ACCOUNTNO, '%Y.%m.%d %T') AS DATE)) AS MaxStart
,a.ACCOUNTNAME AS Mob_AccountName,a.NUMERO_IDENTIFICACION as Mobile_Id
,CAST(a.TOTAL_MRC_D AS DECIMAL) AS Mobile_MRC
,cast(date_parse((case when a.INV_EXP_DT = '0' then null else a.INV_EXP_DT end), '%Y.%m.%d %T') as date) AS MobilePay_Dt
,a.ACCOUNT_STATUS
,b.dias_de_atraso as Over_due
,first_value(b.dias_de_atraso) over(partition by a.ACCOUNTNO,date_trunc('month',date(a.dt)) order by date(a.dt) desc) as Last_Overdue
FROM "db-analytics-prod"."tbl_postpaid_cwp" a left join "db-stage-dev"."polaris_campaigns" b 
on a.accountno = cast(b.billableaccountno as varchar) and a.dt = b.dt
WHERE "biz_unit_d"='B2C' AND a.ACCOUNT_STATUS IN ('ACTIVE','GROSS_ADDS','PORT_IN', 'RESTRICTED') AND a.INV_EXP_DT<>'nan' 
--AND date(dt) between (DATE('2022-11-01') + interval '1' MONTH - interval '1' DAY - interval '3' MONTH) AND  (DATE('2022-11-01') + interval '1' MONTH - interval '1' DAY + interval '3' MONTH)
GROUP BY a.DT,2,3,4,a.ACCOUNTNAME,7,8,9,10,11
)

--Select *from mobileusefulfields where Mobileaccount = '1762634' and MobileMonth = date('2022-11-01')
,NumberRGUsPerUser AS(
SELECT DISTINCT MobileMonth,dt,MobileAccount,count(distinct PHONENUMBER) AS NumRGUs
FROM MobileUsefulFields
GROUP BY MobileMonth,dt,MobileAccount
)
--Select sum (Numrgus) from numberrgusperuser where MobileMonth = date('2022-11-01') --4.751.968
--Select count(*) from numberrgusperuser where MobileMonth = date('2022-11-01') -- 5.125.245
--Select * from NumberRGUsPerUser  where MobileMonth = date('2022-11-01') order by NumRgus desc
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS Month, MobileAccount, Round(avg(Mobile_MRC),0) AS AvgMRC_Mobile
  FROM MobileUsefulFields 
  WHERE Mobile_MRC IS NOT NULL AND Mobile_MRC <> 0
  GROUP BY 1, MobileAccount
)
,MobileActive_BOM AS(
SELECT m.DT AS B_Date, DATE_TRUNC('MONTH', DATE_ADD('MONTH', 1, DATE(m.dt))) AS MobileMonth,
m.MobileAccount as MobileBOM, PhoneNumber as Phone_BOM, MaxStart as Mobile_B_MaxStart
, Mob_AccountName as B_Mob_Acc_Name, Mobile_Id as B_Mobile_ID
, round(Mobile_MRC,0) as B_MobileMRC
, NumRGUs AS B_MobileRGUs, round(AvgMRC_Mobile,0) as B_AvgMobileMRC
, Over_due as B_Overdue
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
WHEN DATE_DIFF('DAY',MaxStart, m.dt)>180 AND DATE_DIFF('DAY',MaxStart, m.dt)<= 360 THEN 'Mid-Tenure'
      WHEN DATE_DIFF('DAY',MaxStart, m.dt)>360 THEN 'Late-Tenure' END AS B_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt)= DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
AND (CAST(Over_due AS INTEGER)<90 OR Over_due IS NULL)
)

,MobileActive_EOM AS(
SELECT m.DT AS E_Date, DATE_TRUNC('MONTH', DATE(m.dt)) AS MobileMonth,
m.MobileAccount as MobileEOM, PhoneNumber as Phone_EOM, MaxStart as Mobile_E_MaxStart
, Mob_AccountName as E_Mob_Acc_Name, Mobile_Id as E_Mobile_ID
, round(Mobile_MRC,0) as E_MobileMRC
, NumRGUs AS E_MobileRGUs, round(AvgMRC_Mobile,0) as E_AvgMobileMRC
, Over_due as E_Overdue
,CASE WHEN DATE_DIFF('DAY',MaxStart, m.dt)<=180 THEN 'Early-Tenure'
WHEN DATE_DIFF('DAY',MaxStart, m.dt)>180 AND DATE_DIFF('DAY',MaxStart, m.dt)<=360 THEN 'Mid-Tenure'
      WHEN DATE_DIFF('DAY', MaxStart, m.dt)>360 THEN 'Late-Tenure' END AS E_MobileTenure
FROM MobileUsefulFields m INNER JOIN NumberRGUsPerUser r ON m.MobileAccount = r.MobileAccount AND m.dt = r.dt
LEFT JOIN AverageMRC_User a ON m.MobileAccount = a.MobileAccount AND  m.MobileMonth = a.Month
WHERE DATE(m.dt) = DATE_TRUNC('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
AND (CAST(Over_due AS INTEGER)<=90 OR Over_due IS NULL)
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
,MainMovementBase AS(
SELECT DISTINCT *
,CASE WHEN (E_MobileRGUs - B_MobileRGUs) = 0 THEN '1.SameRGUs' 
      WHEN (E_MobileRGUs - B_MobileRGUs) > 0 THEN '2.Upsell'
      WHEN (E_MobileRGUs - B_MobileRGUs) < 0 THEN '3.Downsell'
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC('MONTH', Mobile_E_MaxStart) = DATE('2022-11-01')) THEN '4.New Customer'
      WHEN (B_MobileRGUs IS NULL AND E_MobileRGUs > 0 AND DATE_TRUNC('MONTH', Mobile_E_MaxStart) <> DATE('2022-11-01')) THEN '5.Come Back to Life'
      WHEN (B_MobileRGUs > 0 AND E_MobileRGUs IS NULL) THEN '6.Null last day'
      WHEN B_MobileRGUs IS NULL AND E_MobileRGUs IS NULL THEN '7.Always null'
 END AS MobileMainMovement
FROM MobileCustomerStatus
)
,SpinClass AS(
SELECT DISTINCT *, ROUND((E_MobileMRC - B_MobileMRC),0) AS Mobile_MRC_Diff,
      CASE WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)=0 THEN '1.Same'
      WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)>0 THEN '2.Upspin'
      WHEN MobileMainMovement ='1.SameRGUs' AND (E_MobileMRC - B_MobileMRC)<0 THEN '3.Downspin'
      ELSE '4.NoSpin' END AS MobileSpinFlag
FROM MainMovementBase 
)

-- ################################CONVERGENCY#################################################################

,MobileConvergency AS(
SELECT DISTINCT m.*, c.household_id as Mobile_household_id
 ,CASE WHEN Tipo='1. Inscrito a Paquete completo' OR Tipo='2. Beneficio manual' THEN '1.Soft FMC'
       WHEN Tipo='2. Match_ID' OR Tipo='3. Contact number' THEN '2.Near FMC'
       WHEN household_id IS NULL THEN '4. MobileOnly'
       ELSE '3.Mobile-HardBundle' END AS FmcFlagMob
FROM SpinClass m LEFT JOIN Convergente c ON m.PhoneNumber=c.SERVICE_ID AND m.Mobile_Month=c.Mes
)


-- #################################CHURNERS################################################################

,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(dt))) AS MES, MobileAccount AS Account, min(date(dt)) AS FirstCustRecord,date_add('day',-1,min(date(dt))) as PrevFirstCustRecord
    FROM MobileUsefulFields 
    WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
    Group by 1,2
)

,LASTCUSTRECORD as(
    SELECT  DATE_TRUNC('MONTH', DATE(dt)) AS MES, MobileAccount AS Account, max(date(dt)) as LastCustRecord,date_add('day',-1,max(date(dt))) as PrevLastCustRecord,date_add('day',-2,max(date(dt))) as PrevLastCustRecord2
    FROM MobileUsefulFields 
      --WHERE DATE(LOAD_dt) = date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
   Group by 1,2
   order by 1,2
)

,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(dt))) AS MES, MobileAccount AS Account, Over_due
 FROM MobileUsefulFields t
 INNER JOIN FIRSTCUSTRECORD r ON r.account = t.MobileAccount
 WHERE CAST(Over_due as INT) < 90 
 and (date(t.dt) = r.FirstCustRecord or date(t.dt)=r.PrevFirstCustRecord)
 GROUP BY 1, 2, 3
)
--select avg (over_due) from NO_OVERDUE

,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS MES, MobileAccount AS Account, Over_due,
 (date_diff('DAY', DATE(dt), MaxStart)) as ChurnTenureDays
 FROM MobileUsefulFields t
 INNER JOIN LASTCUSTRECORD r ON --date(t.dt) = r.LastCustRecord and 
 r.account = t.MobileAccount
 WHERE (date(t.dt)=r.LastCustRecord or date(t.dt)=r.PrevLastCustRecord or date(t.dt)=r.PrevLastCustRecord2)
 and ( CAST(Over_due AS INTEGER) >= 90)
 GROUP BY 1, 2, 3, 4
 )

--select * from OVERDUELASTDAY limit 10

,MOBILENETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)

--select * from MOBILENETCHURNERS limit 10

,INVOLUNTARYCHURNERS AS(
SELECT DISTINCT i.Month, i.Account AS ChurnAccount, i.ChurnTenureDays
,CASE WHEN i.Account IS NOT NULL THEN '2. Mobile Involuntary Churner' END AS MobileChurnerType
FROM MOBILENETCHURNERS i left join MobileUsefulFields f on i.account=f.MobileAccount and i.month=date_trunc('month',date(f.dt))
where last_overdue >= 90
GROUP BY 1, Account,4, ChurnTenureDays
)

--select count (*) from  involuntarychurners

--select * from INVOLUNTARYCHURNERS limit 10
,FinalMobileChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount--, MobileChurnerType
    FROM InvoluntaryChurners
    WHERE MobileChurnerType = '2. Mobile Involuntary Churner'
)


--select * from FinalMobileChurners limit 10

/*,AllChurners AS(
SELECT DISTINCT Month, ChurnAccount, MobileChurnerType
from FinalMobileChurners
)*/

--select * from AllChurners limit 10

/*,MobileTable_ChurnFlag AS(
SELECT s.*,
CASE WHEN c.ChurnAccount IS NOT NULL THEN '1. Mobile Churner'
WHEN c.ChurnAccount IS NULL THEN '2. Mobile NonChurner'
END AS MobileChurnFlag,
CASE WHEN c.ChurnAccount is not null then MobileChurnerType END AS MobileChurnType
FROM SpinClass s LEFT JOIN AllChurners c ON s.Mobile_Account = c.ChurnAccount  and s.Mobile_Month = c.Month
)*/

--select * from MobileTable_ChurnFlag limit 10

/*,DerecognitionBaseJanuary AS(
SELECT DISTINCT CAST(ACCOUNTNO AS VARCHAR(50)) AS AccountNo, dt as DRCMonth
FROM "lla_cco_int_ext_prod"."cwp_mov_ext_derecognition"
WHERE dt='2022-10-01' AND RGU_DRC = 'Y'
)
,DerecognitionBaseMonth AS(
SELECT '2022-11-01' AS DRCMonth, CAST(ACCOUNTNO AS VARCHAR(50)) AS AccountNo
FROM "lla_cco_int_ext_prod"."cwp_mov_ext_derecognition"
WHERE dt='2022-11-01' AND RGU_DRC = 'Y'
)
,MonthCleaning AS(
SELECT m.*
,CASE WHEN (j.AccountNo IS NOT NULL and MobileMainMovement ='6.Null last day') OR (j.AccountNo IS NOT NULL and d.AccountNo IS NOT NULL) 
THEN 1 ELSE 0 END AS DRC --CONDICION MODIFICADA 12/10/2022
FROM MobileConvergency m 
 LEFT JOIN DerecognitionBaseMonth d ON m.Mobile_Account=d.AccountNo AND m.Mobile_Month=DATE(d.DRCMonth)
 LEFT JOIN DerecognitionBaseJanuary j ON m.Mobile_Account=j.AccountNo AND m.Mobile_Month= date_add('month', 1,DATE(j.DRCMonth))
)
,InvoluntaryChurners_2 AS(
SELECT DISTINCT f.*
FROM DerecognitionBaseMonth f LEFT JOIN DerecognitionBaseJanuary j ON f.AccountNo=j.AccountNo
WHERE j.AccountNo IS NULL
)

select * from InvoluntaryChurners_2 limit 10*/

,ChurnerTypeFlag AS(
SELECT f.*
,CASE WHEN MobileMainMovement='6.Null last day' OR i.ChurnAccount IS NOT NULL THEN '1. Mobile Churner'
      ELSE '2. Mobile NonChurner' END AS MobileChurnFlag
,CASE --WHEN MobileMainMovement='6.Null last day' AND i.ChurnAccount  IS NOT NULL THEN '3. Mobile Mixed Churner'
      WHEN MobileMainMovement='6.Null last day' AND i.ChurnAccount IS NULL THEN '1. Mobile Voluntary Churner' 
      WHEN i.ChurnAccount IS NOT NULL /*AND MobileMainMovement<>'6.Null last day'*/ THEN '2. Mobile Involuntary Churner'
END AS MobileChurnerType
FROM mobileconvergency f LEFT JOIN FinalMobileChurners i ON f.Mobile_Account=i.ChurnAccount AND f.Mobile_Month = DATE(i.Month)
)

--select * from ChurnerTypeFlag limit 20

,FullMobileBase AS(
SELECT DISTINCT Mobile_Month,Mobile_Account,PhoneNumber
--,CASE WHEN Mobile_ActiveBOM=1 AND Mobile_Account in (select accountno from DerecognitionBaseJanuary) THEN 0 ELSE Mobile_ActiveBOM END AS 
,Mobile_ActiveBOM
,CASE WHEN Mobile_ActiveEOM=1 AND MobileChurnFlag='2. Mobile NonChurner' THEN 1
      ELSE 0 END AS Mobile_ActiveEOM
,B_Date,Phone_BOM,Mobile_B_MaxStart,B_Mob_Acc_Name,B_Mobile_ID,B_MobileMRC,B_MobileRGUs,B_AvgMobileMRC,B_MobileTenure
,E_Date,Phone_EOM,Mobile_E_MaxStart,E_Mob_Acc_Name,E_Mobile_ID,E_MobileMRC
,CASE WHEN E_MobileRGUs=1 AND MobileChurnFlag='2. Mobile NonChurner' THEN 1
      ELSE 0 END AS E_MobileRGUs
,E_AvgMobileMRC,E_MobileTenure,Mobile_MRC_Diff
,CASE WHEN MobileChurnFlag='1. Mobile Churner' THEN '6.Null last day'
      ELSE MobileMainMovement END AS MobileMainMovement
,CASE WHEN MobileChurnFlag='1. Mobile Churner' THEN '4.NoSpin'
      ELSE MobileSpinFlag END AS MobileSpinFlag
,mobile_household_id,FmcFlagMob,MobileChurnFlag,MobileChurnerType
FROM ChurnerTypeFlag
)

,InactiveUsers AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD('MONTH',1,date(Mobile_Month)) AS RejoinerMonth
FROM MobileCustomerStatus
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)
,RejoinerPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>=date('2022-11-01') AND RejoinerMonth<=DATE_ADD('MONTH',1,date('2022-11-01')) THEN 1 ELSE 0 END AS Mobile_PRMonth
FROM FullMobileBase f LEFT JOIN InactiveUsers i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)

,MobileRejoinerPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PRMonth,Mobile_Account,date('2022-11-01') AS Month
FROM RejoinerPopulation
WHERE RejoinerPopFlag=1
AND Mobile_PRMonth=1
AND Mobile_Month<>date('2022-11-01')
GROUP BY 1,2,3,4
)

,MonthMobileRejoiners AS(
SELECT f.*,Mobile_PRMonth
,CASE WHEN Mobile_PRMonth=1 AND MobileMainMovement='5.Come Back to Life'
THEN 1 ELSE 0 END AS Mobile_RejoinerMonth
FROM FullMobileBase f LEFT JOIN MobileRejoinerPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month=CAST(r.Month AS DATE)
)

/*
SELECT *
FROM MonthMobileRejoiners
where mobile_month=date('2022-11-01') --and drc=0 
limit 10
*/

select Mobile_Month
        ,mobilechurnertype
        ,count(distinct Mobile_Account) as accounts
        ,sum(b_mobilergus) as BOM_RGUs_mob
        ,sum(e_mobilergus) as EOM_RGUs_mob
from MonthMobileRejoiners
where Mobile_Month = date('2022-11-01')
    and mobilechurnertype IN ('2. Mobile Involuntary Churner','1. Mobile Voluntary Churner')
    --and drc = 0
group by 1,2
order by 2,1
