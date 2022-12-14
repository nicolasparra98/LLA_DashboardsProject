WITH 
UsefulFields AS(
SELECT  DISTINCT DATE_TRUNC ('Month' , cast(dt as date)) AS Month,
        dt,
        act_acct_cd,
        pd_vo_prod_nm, 
        PD_TV_PROD_nm, 
        pd_bb_prod_nm, 
        FI_OUTST_AGE, 
        C_CUST_AGE, 
        first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY dt ASC) AS MinInst,
        first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY ACT_ACCT_INST_DT DESC) AS MaxInst,
        CST_CHRN_DT AS ChurnDate, 
        DATE_DIFF('DAY',cast(OLDEST_UNPAID_BILL_DT as date), 
        cast(dt as date)) AS MORA, 
        ACT_CONTACT_MAIL_1,
        act_contact_phone_1,
        round(FI_VO_MRC_AMT,0) AS mrcVO, 
        round(FI_BB_MRC_AMT,0) AS mrcBB, 
        round(FI_TV_MRC_AMT,0) AS mrcTV,
        round((FI_VO_MRC_AMT + FI_BB_MRC_AMT + FI_TV_MRC_AMT),0) as avgmrc, 
        round(FI_BILL_AMT_M0,0) AS Bill, 
        ACT_CUST_STRT_DT,
        CASE WHEN pd_vo_prod_nm IS NOT NULL and pd_vo_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_VO,
        CASE WHEN pd_tv_prod_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_TV,
        CASE WHEN pd_bb_prod_nm IS NOT NULL and pd_bb_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_BB,
        CASE 
            WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm<>'' AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '3P'
            WHEN (PD_VO_PROD_nm IS NULL or pd_vo_prod_nm ='')  AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm <>'' AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '2P'
            WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND (PD_BB_PROD_nm IS NULL or pd_bb_prod_nm ='') AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '2P'
            WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm <>'' AND (PD_TV_PROD_nm IS NULL or pd_tv_prod_nm ='') THEN '2P'
            WHEN PD_VO_PROD_nm IS NULL AND PD_BB_PROD_nm IS NULL AND PD_TV_PROD_nm IS NULL THEN '0P'
            ELSE '1P' END AS MIX, 
        pd_bb_tech,
        CASE 
            WHEN pd_bb_prod_nm LIKE '%FTTH%' OR pd_tv_prod_nm ='NextGen TV' THEN 'FTTH'
            ELSE 'HFC' END AS TechFlag,
        first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by date(dt) desc) as Last_Overdue
FROM "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and act_acct_stat='ACTIVO' 
)

------------------------------------------------ VOLUNTARY CHURNERS ------------------------------------------------------------------------------------------------------------

,SO_flag AS(
Select distinct 
date_trunc('Month', date(completed_date)) as month,date(completed_date) as EndDate,date(order_start_date) as StartDate
,cease_reason_code, cease_reason_desc,cease_reason_group
,CASE 
 WHEN cease_reason_code IN ('1','3','4','5','6','7','8','10','12','13','14','15','16','18','20','23','25','26','29','30','31','34','35','36','37','38','39','40','41','42','43','45','46','47','50','51','52','53','54','56','57','70','71','73','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91') THEN 'Voluntario'
 WHEN cease_reason_code IN('2','74') THEN 'Involuntario'
 WHEN (cease_reason_code = '9' AND cease_reason_desc='CAMBIO DE TECNOLOGIA') OR (cease_reason_code IN('32','44','55','72')) THEN 'Migracion'
 WHEN cease_reason_code = '9' AND cease_reason_desc<>'CAMBIO DE TECNOLOGIA' THEN 'Voluntario'
ELSE NULL END AS DxType
,account_id
,lob_vo_count,lob_bb_count,lob_tv_count
from "db-stage-dev"."so_hdr_cwp" 
where order_type = 'DEACTIVATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
)
,RGUsFlag_SO AS(
SELECT Month,StartDate,account_id,DxType
,CASE WHEN lob_vo_count>0 THEN 1 ELSE 0 END AS VO_Churn
,CASE WHEN lob_bb_count>0 THEN 1 ELSE 0 END AS BB_Churn
,CASE WHEN lob_tv_count>0 THEN 1 ELSE 0 END AS TV_Churn
FROM SO_FLAG
)
,ChurnedRGUs_SO_Prel AS(
SELECT DISTINCT *
,(VO_CHURN + BB_CHURN + TV_CHURN) AS RGUs_Prel
FROM RGUsFlag_SO
WHERE DxType='Voluntario'
)
,ChurnedRGUs_SO AS (
SELECT DISTINCT Month,Account_id,dxtype
,SUM(RGUs_Prel) AS ChurnedRGUs
FROM ChurnedRGUs_SO_Prel
GROUP BY 1,2,3
)



----------------------------------------------- INVOLUNTARY CHURNERS -----------------------------------------------------------------------------------------------------------

,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(dt))) AS MES, act_acct_cd AS Account, min(date(dt)) AS FirstCustRecord,date_add('day',-1,min(date(dt))) as PrevFirstCustRecord
    FROM UsefulFields 
    WHERE date(dt) = --date('2022-02-28')-- 
    date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
    Group by 1,2
)
,LastCustRecord as(
    SELECT  DATE_TRUNC('MONTH', DATE(dt)) AS MES, act_acct_cd AS Account, max(date(dt)) as LastCustRecord,date_add('day',-1,max(date(dt))) as PrevLastCustRecord,date_add('day',-2,max(date(dt))) as PrevLastCustRecord2
    FROM UsefulFields 
      --WHERE DATE(LOAD_dt) = date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
   Group by 1,2
   order by 1,2
)
 ,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(dt))) AS MES, act_acct_cd AS Account, fi_outst_age
 FROM UsefulFields t
 INNER JOIN FIRSTCUSTRECORD  r ON r.account = t.act_acct_cd
 WHERE CAST(fi_outst_age as INT) <= 90 
 and (date(t.dt) = r.FirstCustRecord or date(t.dt)=r.PrevFirstCustRecord)
 GROUP BY 1, 2, 3
)
 ,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS MES, act_acct_cd AS Account, fi_outst_age,
 (date_diff('DAY', DATE(dt), MaxInst)) as ChurnTenureDays
 FROM UsefulFields t
 INNER JOIN LastCustRecord r ON --date(t.dt) = r.LastCustRecord and 
 r.account = t.act_acct_cd
 WHERE (date(t.dt)=r.LastCustRecord or date(t.dt)=r.PrevLastCustRecord or date(t.dt)=r.PrevLastCustRecord2)
 and CAST(fi_outst_age AS INTEGER) >= 90
 GROUP BY 1, 2, 3, 4
 )
 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT i.Month, i.Account AS ChurnAccount, i.ChurnTenureDays
,CASE WHEN i.Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS FixedChurnerType
FROM INVOLUNTARYNETCHURNERS i left join usefulfields f on i.account=f.act_acct_cd and i.month=date_trunc('month',date(f.dt))
where last_overdue>=90
GROUP BY 1, Account,4, ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount, FixedChurnerType
    FROM InvoluntaryChurners
    WHERE FixedChurnerType = '2. Fixed Involuntary Churner'
)

SELECT DISTINCT MONTH, COUNT (DISTINCT CHURNACCOUNT)
FROM FINALINVOLUNTARYCHURNERS
GROUP BY 1
ORDER BY 1
