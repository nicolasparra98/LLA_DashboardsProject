--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cwc_mob_stg_dashboardInput_feb_PM" AS
WITH
MobileFields AS(
   SELECT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, account_id,dt, phone_no, 
   CASE WHEN IS_NAN (cast(total_mrc_mo as double)) THEN 0
   WHEN NOT IS_NAN  (cast(total_mrc_mo as double)) THEN round(cast(total_mrc_mo as double),0)
   END AS total_mrc_mo, DATE_DIFF('DAY',  date(first_value(account_creation_date) over(partition by account_id order by dt desc)),  date(first_value(dt) over(partition by account_id order by dt desc))) as MaxTenureDays,
   first_value(account_creation_date) over(partition by account_id order by dt desc) AS Mobile_MaxStart,
   cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date) as oldest_unpaid_bill_dt_adj,
   date_diff('DAY',  cast (concat(substr(oldest_unpaid_bill_dt, 1,4),'-',substr(oldest_unpaid_bill_dt, 5,2),'-', substr(oldest_unpaid_bill_dt, 7,2)) as date), cast(dt as date)) as Fi_outst_age,tot_inv_mo, account_status
   FROM "db-analytics-prod"."tbl_postpaid_cwc" 
   WHERE org_id = '338' AND account_type ='Residential'
   AND account_status NOT IN('Ceased','Closed','Recommended for cease')
   -- Variabilizar mes de reporte
   AND date(dt) between (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY)
),

MobileRGUsPerUser AS(
    SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, dt, account_id, count(distinct phone_no) as MobileRGUs
    FROM MobileFields
    GROUP BY Month, dt, account_id
)


 ,AverageMRC_Mobile AS(
  SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, account_id, phone_no
  , round(avg(total_mrc_mo),0) AS AvgMRC_Mobile
  FROM MobileFields
  WHERE total_mrc_mo IS NOT NULL AND total_mrc_mo <> 0 AND NOT IS_NAN(total_mrc_mo)
  GROUP BY 1, account_id,phone_no
  )

,MobileUsersBOM AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE_ADD('MONTH', 1, DATE(m.dt))) AS Mobile_Month, m.account_id AS mobileBOM,m.dt AS Mobile_B_Date ,total_mrc_mo AS Mobile_MRC_BOM, m.phone_no as B_Phone
,MaxTenureDays AS Mobile_B_TenureDays, Mobile_MaxStart as B_Mobile_MaxStart, AvgMRC_Mobile as B_AvgMRC_Mobile, MobileRGUs as B_MobileRGus, Fi_outst_age as B_MobileOutsAge
FROM MobileFields m INNER JOIN MobileRGUsPerUser r ON  m.account_id = r.account_id AND m.dt = r.dt LEFT JOIN AverageMRC_Mobile a ON m.account_id = a.account_id AND m.Month = a.Month and 
m.Phone_no = a.Phone_no
WHERE DATE(m.dt) = date_trunc('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
--AND ((AvgMRC_Mobile IS NOT NULL AND AvgMRC_Mobile <> 0 AND MaxTenureDays >60) OR  (MaxTenureDays <=60)) 
and (fi_outst_age <= 90 or fi_outst_age is null)
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,MobileUsersEOM AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE(m.dt)) AS Mobile_Month, m.account_id AS mobileEOM, m.dt AS Mobile_E_Date ,total_mrc_mo AS Mobile_MRC_EOM, m.phone_no as E_Phone
,MaxTenureDays AS Mobile_E_TenureDays,Mobile_MaxStart as E_Mobile_MaxStart,AvgMRC_Mobile as E_AvgMRC_Mobile, MobileRGUs as E_MobileRGus, Fi_outst_age as E_MobileOutsAge
FROM MobileFields m  INNER JOIN MobileRGUsPerUser r ON  m.account_id = r.account_id AND m.dt = r.dt LEFT JOIN AverageMRC_Mobile a ON m.account_id = a.account_id AND m.Month = a.Month and 
m.Phone_no = a.Phone_no
WHERE DATE(m.dt) = date_trunc('MONTH', DATE(m.dt)) + interval '1' MONTH - interval '1' day
--AND ((AvgMRC_Mobile IS NOT NULL AND AvgMRC_Mobile <> 0 AND MaxTenureDays >60) OR  (MaxTenureDays <=60)) and (fi_outst_age <= 90 or fi_outst_age is null)
GROUP BY 1,2,3,4,5,6,7,8,9,10
)

,MobileCustomerStatus AS(
  SELECT DISTINCT 
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN b.Mobile_Month
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN e.Mobile_Month
  END AS Mobile_Month,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN mobileBOM
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN mobileEOM
  END AS Mobile_Account,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN B_Phone
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN E_Phone
  END AS Mobile_Phone,
  CASE WHEN (mobileBOM IS NOT NULL AND mobileEOM IS NOT NULL) OR (mobileBOM IS NOT NULL AND mobileEOM IS NULL) THEN Mobile_B_TenureDays
      WHEN (mobileBOM IS NULL AND mobileEOM IS NOT NULL) THEN Mobile_E_TenureDays
  END AS TenureDays,
  CASE WHEN mobileBOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
  CASE WHEN mobileEOM IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
  Mobile_B_Date, Mobile_B_TenureDays, B_Mobile_MaxStart,
  CASE WHEN Mobile_B_TenureDays <= 180 THEN 'Early-Tenure'
  WHEN Mobile_B_TenureDays > 180 AND Mobile_B_TenureDays <= 360 THEN 'Mid-Tenure'
  WHEN Mobile_B_TenureDays > 360 THEN 'Late-Tenure' END AS B_MobileTenureSegment,
  Mobile_MRC_BOM, B_AvgMRC_Mobile, B_MobileRGUs, B_MobileOutsage,
  CASE WHEN B_MobileRGUs = 1 THEN 'Single-line'
  WHEN B_MobileRGus > 1 THEN 'Multiple-lines'
  END AS B_MobileCustomerType,
  CASE WHEN E_MobileRGUs = 1 THEN 'Single-line'
  WHEN E_MobileRGus > 1 THEN 'Multiple-lines'
  END AS E_MobileCustomerType,
  Mobile_E_Date, Mobile_E_TenureDays, E_Mobile_MaxStart,
 CASE WHEN Mobile_E_TenureDays <= 180 THEN 'Early-Tenure'
 WHEN Mobile_E_TenureDays > 180 AND  Mobile_E_TenureDays <= 360 THEN 'Early-Tenure'
  WHEN Mobile_E_TenureDays> 360 THEN 'Late-Tenure' END AS E_MobileTenureSegment,
  Mobile_MRC_EOM, E_AvgMRC_Mobile, E_MobileRGus, E_MobileOutsAge
  FROM MobileUsersBOM b FULL OUTER JOIN MobileUsersEOM e
  ON b.mobileBOM = e.mobileEOM AND b.Mobile_Month= e.Mobile_Month
  AND b.B_Phone = e.E_Phone
)

,MobileMovementClass AS( 
SELECT DISTINCT *,
     CASE WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 THEN '1.Mantain'
     WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN '2.Loss'
     WHEN Mobile_ActiveBOM =0 AND Mobile_ActiveEOM =1 AND  DATE_TRUNC ('MONTH',DATE(E_Mobile_MaxStart)) = DATE('2022-09-01') THEN '3.New Customer'
     WHEN Mobile_ActiveBOM =0 AND Mobile_ActiveEOM =1 AND  DATE_TRUNC ('MONTH',DATE(E_Mobile_MaxStart)) <> DATE('2022-09-01') THEN '4.Come Back to Life'
     ELSE '5.Null' END AS MobileMovementFlag,
     CASE WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND B_MobileRGus > E_MobileRGus THEN 'Downsell'
     WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND B_MobileRGus > E_MobileRGus THEN 'Upsell'
     WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND B_MobileRGus = E_MobileRGus THEN 'No Change'
     ELSE Null END AS Mobile_SecondaryMovementFlag
FROM MobileCustomerStatus
)

,SpinClass AS(
SELECT DISTINCT *, (Mobile_MRC_EOM - Mobile_MRC_BOM) AS Mobile_MRC_Diff,
      CASE WHEN MobileMovementFlag ='1.Mantain' AND (Mobile_MRC_EOM - Mobile_MRC_BOM)=0 THEN '1.NoSpin'
      WHEN MobileMovementFlag ='1.Mantain' AND (Mobile_MRC_EOM - Mobile_MRC_BOM)> 75 THEN '2.Upspin'
      WHEN MobileMovementFlag ='1.Mantain' AND (Mobile_MRC_EOM - Mobile_MRC_BOM)< -75 THEN '3.Downspin'
      ELSE '1.NoSpin' END AS SpinFlag
FROM MobileMovementClass 
)

,MobileBase_ChurnFlag AS(
SELECT DISTINCT *
FROM SpinClass
)

------------------------- Churners ------------------------------------------------

------------------------ Voluntary Churners ---------------------------------------
,mobile_so AS(
SELECT *, DATE_TRUNC('month', date(completed_date)) as ChurnMonth
FROM "db-stage-dev"."so_hdr_cwc" 
 WHERE
        org_cntry = 'Jamaica'
        AND cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary')
        AND network_type IN ('LTE','MOBILE')
        AND order_status = 'COMPLETED'
        AND account_type = 'Residential'
        AND order_type = 'DEACTIVATION'
        --- variabilizar mes de reporte
        AND DATE_TRUNC('month', date(completed_date)) = date('2022-09-01')
)

,Voluntary_Churn AS(
SELECT DISTINCT ChurnMonth,account_id
FROM mobile_so
WHERE  cease_reason_group = 'Voluntary'
)

,VoluntaryChurners AS(
SELECT ChurnMonth as Month, account_id as account, 
'1. Mobile Voluntary Churner' as ChurnType
FROM Voluntary_Churn
GROUP BY 1,2,3
)

-------------------- Involuntary Churners ----------------------------------------

,CUSTOMERS_FIRSTLAST_RECORD AS(
 SELECT DISTINCT Month as Mes, account_id AS Account, Min(dt) as FirstCustRecord, Max(dt) as LastCustRecord
 FROM MobileFields
 GROUP BY 1, 2
),

NO_OVERDUE AS(
 SELECT DISTINCT Month as Mes, Account_id AS Account, fi_outst_age
 FROM MobileFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.FirstCustRecord and r.account = t.account_id
 WHERE cast(fi_outst_age as double) <= 90
 GROUP BY 1, 2, fi_outst_age
),

OVERDUELASTDAY AS(
 SELECT DISTINCT Month AS MES, account_id AS Account, fi_outst_age,
 (date_diff('day', DATE(Mobile_MaxStart),DATE(dt))) as ChurnTenureDays
 FROM MobileFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.LastCustRecord and r.account = t.account_id
 WHERE  cast(fi_outst_age as double) >= 90
 GROUP BY 1, 2, fi_outst_age, 4
),

INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.Mes AS Month, n. account, l.ChurnTenureDays, l.fi_outst_age
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, cast(Account AS varchar) AS Account
,CASE WHEN Account IS NOT NULL THEN '2. Mobile Involuntary Churner' END AS ChurnType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY 1,2,3
)

,AllMobileChurners AS(
SELECT DISTINCT Month,Account,ChurnType
from (SELECT Month,cast(Account as double) as account,ChurnType from VoluntaryChurners a 
      UNION ALL
      SELECT Month,cast(Account as double) as account,ChurnType from InvoluntaryChurners b)
)


,MobileBase_AllFlags AS(
SELECT DISTINCT m.*, 
CASE WHEN c.account IS NOT NULL THEN '1. Mobile Churner'
ELSE '2. Mobile NonChurner' END AS MobileChurnFlag,
case WHEN c.account IS NOT NULL THEN ChurnType
ELSE  '2. Mobile NonChurner' END AS MobileChurnType,
    CASE WHEN m.TenureDays <= 180 THEN 'Early-life'
    WHEN m.TenureDays > 180 and m.TenureDays <= 360  THEN 'Mid-life'
    WHEN m.TenureDays > 360 THEN 'Late-life'
    END AS MobileChurnTenureSegment
FROM MobileBase_ChurnFlag m LEFT JOIN AllMobileChurners c ON cast(m.mobile_account as double)=cast(c.account as double) AND Mobile_Month= Month
)

-------------------Early Dx Flag -------------------------------------------------------

,join_so_mobilebase as (
    select a.*, 
    case when a.MobileChurnType ='1. Mobile Voluntary Churner' then 'Voluntary' 
    when a.MobileChurnType ='2. Mobile Involuntary Churner'then 'Involuntary'
    when a.MobileChurnType ='2. Mobile NonChurner' and Mobile_ActiveEOM = 0 and cast(
    a.B_MobileOutsAge as integer) <90 and ((length(a.mobile_account) = 12) OR (b.cease_reason_group = 'Involuntary' AND length(a.mobile_account) = 8)) then 'Early Dx'
    end as FinalMobileChurnFlag
    from MobileBase_AllFlags a left join mobile_so b
    on cast(a.mobile_account as varchar) = cast(b.account_id as varchar)
)

----------------------------Rejoiners-------------------------------------------
,InactiveUsers AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD('MONTH', 1, Mobile_Month) AS RejoinerMonth
FROM MobileCustomerStatus
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)

,RejoinerPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>= DATE('2022-09-01')  AND RejoinerMonth<=DATE_ADD('MONTH',1,DATE('2022-09-01')) THEN 1 ELSE 0 END AS Mobile_PRMonth
FROM MobileBase_AllFlags f LEFT JOIN InactiveUsers i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)

,MobileRejoinerMonthPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PRMonth,Mobile_Account,DATE('2022-09-01') AS Month
FROM RejoinerPopulation
WHERE RejoinerPopFlag=1
AND Mobile_PRMonth=1
AND Mobile_Month<> DATE('2022-09-01')
GROUP BY 1,2,3,4
)

,FullMobileBase_Rejoiners AS(
SELECT f.*,Mobile_PRMonth
,CASE WHEN Mobile_PRMonth=1 AND MobileMovementFlag ='4.Come Back to Life'
THEN 1 ELSE 0 END AS Mobile_RejoinerMonth
FROM join_so_mobilebase f LEFT JOIN MobileRejoinerMonthPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month= CAST(r.Month AS DATE)
)

,fmctable as (select  mobile_account from "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where month = date(dt) and month = date('2022-09-01') and finalmobilechurnflag is null)


,mrcnull as (SELECT finalmobilechurnflag, mobile_account
FROM FullMobileBase_Rejoiners
where mobile_month = date('2022-09-01') and finalmobilechurnflag is null-- group by 1 order by 1
)

,joint as (select a.mobile_account as mrcaccount, b.mobile_account as fmcaccount, '2022-09-01' as month --dt, act_acct_cd, fi_bill_amt_m0, fi_bill_amt_m1, lst_pymt_dt, fi_tot_mrc_amt, fi_bb_mrc_amt, fi_tv_mrc_amt, fi_vo_mrc_amt, fi_outst_age,pd_mix_cd, pd_mix_nm,ACT_ACCT_STAT   
from mrcnull a left join fmctable b on a.mobile_account =b.mobile_account )

,cuentas_a_evaluar as (select distinct mrcaccount from joint where fmcaccount is null)

,AverageMRC_Mobile_v2 AS(
  SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS Month, account_id, phone_no
  , round(avg(total_mrc_mo),0) AS AvgMRC_Mobile
  FROM MobileFields
  WHERE total_mrc_mo IS NOT NULL AND total_mrc_mo <> 0 AND NOT IS_NAN(total_mrc_mo)
  GROUP BY 1, account_id,phone_no
  )

,JoinTable as(
select A.*, C.AvgMRC_Mobile
from  MobileFields A right join Cuentas_A_Evaluar B on A.account_id=B.mrcaccount
left join AverageMRC_Mobile_v2 C on A.account_id =C.account_id and C.Month = A.Month 
where date(A.dt)>=date('2022-08-01') 
)

Select distinct dt, account_id, AvgMRC_Mobile, total_mrc_mo,Fi_outst_age,MaxTenureDays, tot_inv_mo,account_status,bandera
--distinct dt, count(distinct account_id) 
--distinct month, Bandera, count (distinct account_id) as count
--distinct month, dt, Bandera, account_id
from(
select *, case when AvgMRC_Mobile <> 0 or AvgMRC_Mobile is not null then 'OK MRC' else 'MRC null or 0 all month' end as bandera
from JoinTable
)
--group by 1 
where date(dt) = date('2022-08-31') --and maxtenuredays>60
group by 1,2,1,3,4,5,6,7,8,9
order by 1,2,1,3,4


--select dt, sum(CASE WHEN IS_NAN (cast(total_mrc_mo as double)) THEN 0
--   WHEN NOT IS_NAN  (cast(total_mrc_mo as double)) THEN round(cast(total_mrc_mo as double),0)
--   END ) AS total_mrc_mo FROM "db-analytics-prod"."tbl_postpaid_cwc" where date(dt) >= date('2022-04-01') group by 1 order by 1
