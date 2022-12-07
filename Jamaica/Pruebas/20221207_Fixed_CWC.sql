--create table  "lla_cco_int_san"."cwc_fix_stg_dashboardInput_sep_AL_new_mrc" as
WITH
  UsefulFields AS(
  SELECT DATE_TRUNC('MONTH',DATE(dt)) AS Month, first_value(dt) over(partition by act_acct_cd, date_trunc('Month', date(dt)) order by dt desc) as MaxDateMonth,
    dt,act_acct_cd, act_contact_phone_1, act_contact_phone_2, act_contact_phone_3,
    pd_mix_cd,pd_mix_nm,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,
   CASE WHEN IS_NAN (cast(fi_tot_mrc_amt AS double)) THEN 0
    WHEN NOT IS_NAN (cast(fi_tot_mrc_amt AS double)) THEN ROUND((cast(fi_tot_mrc_amt AS double)),0)
    END AS mrc_amt,
    CASE WHEN IS_NAN (cast(fi_bill_amt_m0  AS double)) THEN 0
    WHEN NOT IS_NAN (cast(fi_bill_amt_m0  AS double)) THEN ROUND((cast(fi_bill_amt_m0  AS double)),0)
    END AS bill_amtM0,
    CASE WHEN IS_NAN (cast(fi_bill_amt_m1 AS double)) THEN 0
    WHEN NOT IS_NAN (cast(fi_bill_amt_m1 AS double)) THEN ROUND((cast(fi_bill_amt_m1 AS double)),0)
    END AS bill_amtM1,
    CASE WHEN fi_outst_age IS NULL THEN -1 ELSE cast(fi_outst_age as integer) end as fi_outst_age
    , fi_tot_srv_chrg_amt, ROUND(cast(fi_bb_mrc_amt as double),0) as fi_bb_mrc_amt, ROUND(cast(fi_tv_mrc_amt as double),0) as fi_tv_mrc_amt, ROUND(cast(fi_vo_mrc_amt as double),0) as fi_vo_mrc_amt,
    first_value(act_cust_strt_dt) over(partition by act_acct_cd order by dt desc) AS MaxStart, bundle_code, bundle_name,
    CASE WHEN (pd_mix_nm like '%BO%') THEN 1 ELSE 0 END AS numBB,
   CASE WHEN (pd_mix_nm like '%TV%') THEN 1 ELSE 0 END AS numTV,
   CASE WHEN (pd_mix_nm like '%VO%') THEN 1 ELSE 0 END AS numVO,
  CASE WHEN length(cast(act_acct_cd as varchar))=8 then 'HFC' 
            WHEN NR_FDP<>'' and NR_FDP<>' ' and NR_FDP is not null THEN 'FTTH' 
            WHEN pd_vo_tech='FIBER' THEN 'FTTH' 
            WHEN (pd_bb_prod_nm like '%GPON%'  OR pd_bb_prod_nm like '%FTT%') and 
            (pd_bb_prod_nm not like '%ADSL%' and pd_bb_prod_nm not like '%VDSL%') THEN 'FTTH' 
            ELSE 'COPPER' END AS Techonology_type,
  cst_cust_cd
  FROM "db-analytics-prod"."tbl_fixed_cwc" 
  WHERE
    org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard')
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
    -- El mes toca variabilizarlo (mes de reporte)
   AND date(dt) between (DATE('2022-11-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-11-01') + interval '1' MONTH - interval '1' DAY)
)
-- select * from UsefulFields where act_acct_cd = '285251320000' order by dt desc
,
  AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, act_acct_cd,--avgmrc,avgbillm1,avgbillm0,
  round(avg(mrc_amt),0)  AS AvgMRC,
  round(avg(bill_amtM1),0)  AS AvgBillM1,
  round(avg(bill_amtM0),0)  AS AvgBillM0,MaxDateMonth,MaxStart
  FROM UsefulFields
  --WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY 1, act_acct_cd,MaxDateMonth,MaxStart
  )
  
  , filterbill as ( select *, case when ((AvgBillM1 IS NOT NULL AND AvgBillM1 <> 0 and date_diff('day', DATE(MaxStart), DATE(MaxDateMonth))>60) OR  (date_diff('day',DATE(MaxStart), DATE(MaxDateMonth)) <=60))
      
      or (AvgBillM0 IS NOT NULL AND AvgBillM0 <> 0 AND  date_diff('day', DATE(MaxStart), DATE(MaxDateMonth))>60)
      then 1 else 0 end as bill_filter  from AverageMRC_User )
  
  
  
  ,LastDayRGUs AS(
  SELECT act_acct_cd, first_value (numBB + numTV + numVO) over (partition by act_acct_cd order by dt desc) as last_rgus
  FROM USEFULFIELDS
  )
  
  ,ActiveUsersBOM AS(
  SELECT
    DISTINCT DATE_TRUNC('MONTH', DATE_ADD('MONTH', 1, DATE(u.dt))) AS Month, u.act_acct_cd AS accountBOM, act_contact_phone_1 as PhoneBOM1, act_contact_phone_2 as PhoneBOM2, act_contact_phone_3 as PhoneBOM3,
     u.dt as B_Date,pd_mix_cd as B_MixCode ,pd_mix_nm as B_MixName ,pd_bb_prod_nm as B_ProdBBName,pd_tv_prod_nm as B_ProdTVName,pd_vo_prod_nm as B_ProdVoName,
    (NumBB+NumTV+NumVO) as B_NumRGUs, 
    CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS B_MixName_Adj,
    CASE WHEN NumBB = 1 THEN u.act_acct_cd ELSE NULL END As BB_RGU_BOM,
    CASE WHEN NumTV = 1 THEN u.act_acct_cd ELSE NULL END As TV_RGU_BOM,
    CASE WHEN NumVO = 1 THEN u.act_acct_cd ELSE NULL END As VO_RGU_BOM,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS B_MixCode_Adj,
    mrc_amt as B_MRC ,fi_outst_age  as B_OutstAge, fi_tot_srv_chrg_amt as B_MRCAdj, fi_bb_mrc_amt as B_MRCBB, fi_tv_mrc_amt as B_MRCTV, fi_vo_mrc_amt as B_MRCVO,
    u.MaxStart as B_MaxStart, Techonology_type as B_Tech_Type, bundle_code as B_bundlecode, bundle_name as B_bundlename, AvgMRC as B_Avg_MRC, AvgBillM1 as B_Avg_Bill1,AvgBillM0 as B_Avg_Bill0,
    min(last_rgus) as last_rgus
 FROM
    UsefulFields u LEFT JOIN filterbill a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month LEFT JOIN lastdayRGUs c ON u.act_acct_cd= c.act_acct_cd
  WHERE
    (cast(fi_outst_age AS double) <= 90
      OR fi_outst_age IS NULL)
    AND 
    DATE(u.dt) = date_trunc('MONTH', DATE(u.dt)) + interval '1' MONTH - interval '1' day
    and bill_filter = 1
    
  GROUP BY
    1, 2, 3, 4, 5,6,7 ,8, 9,10,11, 
    15 ,16, 17, 18, 19, 20, 21, 22, 12, 21, 22, 23, 13, 14, 24, 25,26,27,28,29,30
)

,
  ActiveUsersEOM AS ( 
  SELECT
    DISTINCT DATE_TRUNC('MONTH',DATE(u.dt)) AS Month, u.act_acct_cd AS accountEOM, act_contact_phone_1 as PhoneEOM1, act_contact_phone_2 as PhoneEOM2, act_contact_phone_3 as PhoneEOM3,
    u.dt as E_Date,pd_mix_cd as E_MixCode ,pd_mix_nm as E_MixName ,pd_bb_prod_nm as E_ProdBBName,pd_tv_prod_nm as E_ProdTVName,pd_vo_prod_nm as E_ProdVoName,
     (NumBB+NumTV+NumVO) as E_NumRGUs,
     CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN 'BO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN 'TV'
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN 'VO'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN 'BO+TV'
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN 'BO+VO'
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN 'VO+TV'
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN 'BO+VO+TV'
    END AS E_MixName_Adj,
        CASE WHEN NumBB = 1 THEN u.act_acct_cd ELSE NULL END As BB_RGU_EOM,
    CASE WHEN NumTV = 1 THEN u.act_acct_cd ELSE NULL END As TV_RGU_EOM,
    CASE WHEN NumVO = 1 THEN u.act_acct_cd ELSE NULL END As VO_RGU_EOM,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN '1P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN '2P'
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN '3P' END AS E_MixCode_Adj,
     mrc_amt as E_MRC ,fi_outst_age  as E_OutstAge, fi_tot_srv_chrg_amt as E_MRCAdj, fi_bb_mrc_amt as E_MRCBB, fi_tv_mrc_amt as E_MRCTV, fi_vo_mrc_amt as E_MRCVO,
    u.MaxStart as E_MaxStart, Techonology_type as E_Tech_TypE, bundle_code as E_bundlecode, bundle_name as E_bundlename, AvgMRC as E_Avg_MRC,AvgBillM1 as E_Avg_Bill1,AvgBillM0 as E_Avg_Bill0
 FROM
    UsefulFields u LEFT JOIN filterbill a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
  WHERE
    (cast(fi_outst_age AS double) <= 90
      OR fi_outst_age IS NULL)
    AND 
    DATE(u.dt) = date_trunc('MONTH', DATE(u.dt)) + interval '1' MONTH - interval '1' day
    
     AND bill_filter = 1
  GROUP BY
    1, 2, 3, 4, 5, 6,7 ,8, 9,10,11, 
    15 ,16, 17,18, 19, 20, 21, 22, 12, 22, 23, 24, 13, 14,25,26,27,28,29,30
)
,
  CUSTOMERBASE AS(
  SELECT DISTINCT
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Fixed_Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM1
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM1
  END AS f_contactphone1,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM2
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM2
  END AS f_contactphone2,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM3
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM3
  END AS f_contactphone3,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  B_Date,B_Tech_Type, B_MixCode, B_MixCode_Adj, B_MixName, B_MixName_Adj,  B_ProdBBName,B_ProdTVName,B_ProdVoName, BB_RGU_BOM, TV_RGU_BOM, VO_RGU_BOM,B_NumRGUs,B_bundlecode, B_bundlename,
  B_MRC ,B_OutstAge, B_MRCAdj, B_MRCBB, B_MRCTV, B_MRCVO, B_Avg_MRC, b_Avg_Bill1, b_Avg_Bill0,B_MaxStart, DATE_DIFF('day', DATE(B_MaxStart),DATE(B_Date)) as B_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) > 180 AND DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) <= 360 Then 'Mid-Tenure'
  WHEN DATE_DIFF('day', DATE(B_MaxStart), DATE(B_Date)) > 360 THEN 'Late-Tenure' END AS B_FixedTenureSegment,
  E_Date,E_Tech_Type, E_MixCode, E_MixCode_Adj ,E_MixName, E_MixName_Adj ,E_ProdBBName,E_ProdTVName,E_ProdVoName,BB_RGU_EOM, TV_RGU_EOM, VO_RGU_EOM, E_NumRGUs, E_bundlecode, E_bundlename,
  case when (E_MRC = 0 or E_mrc is null) then B_mrc else E_mrc end as E_MRC ,E_OutstAge, E_MRCAdj, E_MRCBB, E_MRCTV, E_MRCVO, E_Avg_MRC, E_Avg_Bill1, E_Avg_Bill0, E_MaxStart, DATE_DIFF('day', DATE(E_MaxStart),  DATE(E_Date)) as E_TenureDays,
  CASE WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) <= 180 Then 'Early-Tenure'
  WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) > 180 AND  DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) <= 360 THEN 'Mid-Tenure'
  WHEN DATE_DIFF('day', DATE(E_MaxStart), DATE(E_Date)) > 360 THEN 'Late-Tenure' END AS E_FixedTenureSegment,
  last_rgus
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
  ORDER BY Fixed_Account
)

,MAINMOVEMENTBASE AS(
SELECT a.*,
(E_MRC - B_MRC) as MRCDiff, 
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN '1.SameRGUs' 
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN '2.Upsell'
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN '3.Downsell'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) = DATE('2022-11-01')) THEN '4.New Customer'
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', DATE(E_MaxStart)) <> DATE('2022-11-01')) THEN '5.Come Back to Life'
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN '6.Null last day'
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN '7.Always null'
END AS mainmovement_raw
FROM CUSTOMERBASE a
),

SPINMOVEMENTBASE AS(
  SELECT b.*,
  CASE WHEN mainmovement_raw = '1.SameRGUs' AND (E_MRC - B_MRC) > 0 THEN '1. Up-spin'
  WHEN mainmovement_raw = '1.SameRGUs' AND (E_MRC - B_MRC) < 0 THEN '2. Down-spin'
  ELSE '3. No Spin' END AS SpinMovement
  FROM mainmovementBASE b
)
--################################# FIXED CHURN FLAGS --###############################################################

,panel_so as (
    select account_id, order_id,
    case when max(lob_vo_count)> 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end as vol_lob_vo_count, 
    case when max(lob_bb_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_bb_count, 
    case when max(lob_tv_count) > 0 and max(cease_reason_group) = 'Voluntary' and order_type = 'DEACTIVATION' then 1 else 0 end  as vol_lob_tv_count, 
   -- case when max(lob_other_count) > 0 then 1 else 0 end  as vol_lob_other_count,
    --DATE_TRUNC('month',  order_start_date) as completed_month,
    DATE_TRUNC('month', completed_date) as completed_month, completed_date,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type,
    lob_VO_count, lob_BB_count, lob_TV_count, customer_id
    from (
        select * FROM "db-stage-dev"."so_hdr_cwc"
    WHERE org_cntry = 'Jamaica'
        AND (cease_reason_group in ('Voluntary', 'Customer Service Transaction', 'Involuntary') or cease_reason_group is null)
        AND (network_type NOT IN ('LTE','MOBILE') or network_type is null)
        AND order_status = 'COMPLETED' AND account_type = 'Residential')
        --AND order_type = 'DEACTIVATION'
        --AND DATE_TRUNC('month', completed_date) = ( select month_analysis from parameters))
    group by account_id, order_id, lob_vo_count, lob_bb_count, lob_tv_count, DATE_TRUNC('month', completed_date), completed_date, customer_id,
    cease_reason_group,org_cntry,order_status,network_type, order_type, account_type
    order by completed_month, account_id, order_id)

-------------------Voluntary Churners-----------------------------------
-- Voluntary churners base
,VOLCHURNERS_SO AS (
SELECT *,
CASE WHEN lob_vo_count > 0 THEN 1 ELSE 0 END AS VO_Churn,
CASE WHEN lob_BB_count > 0 THEN 1 ELSE 0 END AS BB_Churn,
CASE WHEN lob_TV_count > 0 THEN 1 ELSE 0 END AS TV_Churn
FROM panel_so
WHERE
    org_cntry = 'Jamaica' 
    AND cease_reason_group in ('Voluntary')
    AND network_type NOT IN ('LTE','MOBILE') AND order_status = 'COMPLETED'
    AND account_type = 'Residential'
)
-- Number of churned RGUs on the maximum date - it doesn't consider mobile
,ChurnedRGUS_SO AS(
 SELECT *,
 (VO_CHURN + BB_CHURN + TV_CHURN) AS ChurnedRGUs
 FROM VOLCHURNERS_SO
)
-- Number of RGUs a customer has on the last record of the month
,RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, act_acct_cd, cst_cust_cd,
CASE WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('VO', 'BO', 'TV') THEN 1
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('BO+VO', 'BO+TV', 'VO+TV') THEN 2
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(dt)) order by dt) IN ('BO+VO+TV') THEN 3
ELSE 0 END AS NumRgusLastRecord
FROM Usefulfields
WHERE (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
 ORDER BY act_acct_cd
),
-- Date of the last record of the month per customer
LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, act_acct_cd,max(dt) as LastDate, cst_cust_cd
FROM Usefulfields
WHERE  (cast(fi_outst_age as double) <= 90 OR fi_outst_age IS NULL) 
 GROUP BY 1, act_acct_cd, cst_cust_cd
 ORDER BY act_acct_cd
),
-- Number of outstanding days on the last record date
OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC('MONTH',DATE(dt)) AS Month, t.act_acct_cd, fi_outst_age as LastOverdueRecord, t.cst_cust_cd,
(date_diff('day', DATE(dt), DATE(MaxStart))) as ChurnTenureDays
FROM Usefulfields t 
INNER JOIN LastRecordDateDNA d ON t.act_acct_cd = d.act_acct_cd AND t.dt = d.LastDate
),
-- Total Voluntary Churners considering number of churned RGUs, outstanding age and churn date
VoluntaryTotalChurners AS(
SELECT distinct l.Month, l.act_acct_cd, d.LastDate, o.ChurnTenureDays,
CASE WHEN length(cast(l.act_acct_cd AS varchar)) = 12 THEN '1. Liberate'
ELSE '2. Cerilion' END AS BillingSystem,
CASE WHEN (DATE(d.LastDate) = date_trunc('Month', DATE(d.LastDate)) or DATE(d.LastDate) = date_trunc('MONTH', DATE(d.LastDate)) + interval '1' MONTH - interval '1' day) THEN '1. First/Last Day Churner'
ELSE '2. Other Date Churner' END AS ChurnDateType,
CASE WHEN cast(LastOverdueRecord as double) >= 90 THEN '2.Fixed Mixed Churner'
ELSE '1.Fixed Voluntary Churner' END AS ChurnerType
FROM CHURNEDRGUS_SO v INNER JOIN RGUSLastRecordDNA l ON cast(v.customer_id as double) = cast(l.cst_cust_cd as double)
AND v.ChurnedRGUs >= l.NumRgusLastRecord 
AND DATE_TRUNC('month', v.completed_date) = l.Month
INNER JOIN LastRecordDateDNA d on cast(l.act_acct_cd as double)= cast(d.act_acct_cd as double) AND l.Month = d.Month
INNER JOIN OverdueLastRecordDNA o ON cast(l.act_acct_cd as double) = cast(o.act_acct_cd as double) AND l.month = o.Month
WHERE cease_reason_group = 'Voluntary'
)
,VoluntaryChurners AS(
SELECT Month, cast(act_acct_cd AS varchar) AS Account, ChurnerType, ChurnTenureDays
FROM VoluntaryTotalChurners 
WHERE ChurnerType='1.Fixed Voluntary Churner'
GROUP BY Month, act_acct_cd, ChurnerType, ChurnTenureDays
)
---------------------------------Involuntary Churners---------------------------------
,CUSTOMERS_FIRSTLAST_RECORD AS(
 SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS MES, act_acct_cd AS Account, Min(dt) as FirstCustRecord, Max(dt) as LastCustRecord
 ,try(filter(array_agg(cast(fi_outst_age as int) order by dt desc), x->x != -1)[1]) as fi_outst_age_v2
 FROM UsefulFields
 GROUP BY 1, 2
),
NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS MES, act_acct_cd AS Account, fi_outst_age
 FROM UsefulFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.FirstCustRecord and r.account = t.act_acct_cd
 WHERE cast(fi_outst_age as double) <= 90
 GROUP BY 1, 2, fi_outst_age
),
OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC ('MONTH',DATE(dt)) AS MES, act_acct_cd AS Account, fi_outst_age,
 (date_diff('day', DATE(MaxStart), DATE(dt))) as ChurnTenureDays
 FROM UsefulFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.LastCustRecord and r.account = t.act_acct_cd
 WHERE  cast(fi_outst_age as double) >= 90 or (fi_outst_age_v2 >= 90 and date(LastCustRecord) < DATE_TRUNC ('MONTH',DATE(dt)) + interval '1' MONTH - interval '1' DAY)
 GROUP BY 1, 2, fi_outst_age, 4
),
INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, cast(Account AS varchar) AS Account, ChurnTenureDays
,CASE WHEN Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS ChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY Month, 2,4, ChurnTenureDays
)
,AllChurners AS(
SELECT DISTINCT Month,Account,ChurnerType, ChurnTenureDays
from (SELECT Month,Account,ChurnerType, ChurnTenureDays from VoluntaryChurners a 
      UNION ALL
      SELECT Month,Account,ChurnerType, ChurnTenureDays from InvoluntaryChurners b)
),

FixedBase_AllFlags AS(
SELECT s.* ,
CASE WHEN c.account IS NOT NULL THEN '1. Fixed Churner'
ELSE '2. Non-churner' END AS FixedChurnFlag,
case WHEN c.account IS NOT NULL THEN ChurnerType
ELSE '2.Non-Churners' END AS FixedChurnTypeFlag,
ChurnTenureDays, CASE WHEN ChurnTenureDays <= 180 Then '0.Early-tenure Churner'
WHEN ChurnTenureDays > 180 and ChurnTenureDays <= 360 Then '1.Mid-tenure Churner'
WHEN ChurnTenureDays > 360 THEN '2.Late-tenure Churner'
WHEN ChurnTenureDays IS NULL then '3.Non-Churner'
END AS ChurnTenureSegment
FROM SPINMOVEMENTBASE s LEFT JOIN AllChurners c ON cast(s.Fixed_Account as bigint) = cast(c.Account AS bigint) AND s.Fixed_Month = c.Month
)


-----------------------------Churn atypical flags------------------------------
,SO_LLAFlags AS(
 select completed_month, account_id,
   sum(vol_lob_vo_count) + sum(vol_lob_bb_count) + sum(vol_lob_tv_count) --+ sum(vol_lob_other_count) 
   as vol_churn_rgu,
    case when sum(case when cease_reason_group = 'Customer Service Transaction' OR (cease_reason_group is null) then 1 else 0 end) > 0 then 1 else 0 end as cst_churn_flag,
    case when sum(case when cease_reason_group = 'Involuntary' then 1 else 0 end) > 0 then 1 else 0 end as non_pay_so_flag
    from panel_so
    group by account_id, completed_month
)

,join_so_fixedbase as (
    select a.*, 
    case when a.FixedChurnTypeFlag ='1.Fixed Voluntary Churner' and coalesce(e_numrgus,0) < coalesce(b_numrgus,0) then 'Voluntary' 
    when a.mainmovement_raw = '6.Null last day' and a.FixedChurnTypeFlag = '2.Non-Churners' and ((B_outstage = -1 and length(a.fixed_account) = 12) or ((b.non_pay_so_flag = 0 or b.non_pay_so_flag is null) AND length(a.fixed_account) = 8)) then 'Incomplete CST'
    when (b.cst_churn_flag = 1 and a.FixedChurnTypeFlag <>'2. Fixed Involuntary Churner' and a.FixedChurnTypeFlag <> '1.Fixed Voluntary Churner' AND coalesce(B_NumRGUs,0) > coalesce(E_NumRgus,0)) or 
    a.mainmovement_raw = '3.Downsell' then 'CST Churner'
    when a.FixedChurnTypeFlag ='2. Fixed Involuntary Churner' then 'Involuntary'
    when a.FixedChurnTypeFlag ='2.Non-Churners' and ActiveEOM = 0 and cast(a.B_OutstAge as integer) <90 and (b.cst_churn_flag = 0 or b.cst_churn_flag is null) then 'Early Dx'
    --and ((length(a.fixed_account) = 12) OR (b.non_pay_so_flag = 1 AND length(a.fixed_account) = 8)) 
    end as FinalFixedChurnFlag
    from FixedBase_AllFlags a left join SO_LLAFlags b
    on cast(a.fixed_account as varchar) = cast(b.account_id as varchar)
    and a.fixed_month = b.completed_month )

/*select *
from join_so_fixedbase
where fixed_month = date('2022-11-01')
--group by 1*/

----------------------Rejoiners----------------------------------------------

,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH', 1, Fixed_Month) AS RejoinerMonth
FROM CUSTOMERBASE
WHERE ActiveBOM=1 AND ActiveEOM=0
)

,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
,CASE WHEN RejoinerMonth>= DATE('2022-11-01') AND RejoinerMonth<=DATE_ADD('MONTH', 1, DATE('2022-11-01')) THEN 1 ELSE 0 END AS Fixed_PRMonth
FROM CustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)

,FixedRejoinerMonthPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PRMonth,Fixed_Account,DATE('2022-11-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PRMonth=1
AND Fixed_Month<>DATE('2022-11-01')
GROUP BY 1,2,3,4
)

,FullFixedBase_Rejoiners AS(
SELECT f.*,Fixed_PRMonth
,CASE WHEN Fixed_PRMonth=1 AND mainmovement_raw='5.Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_RejoinerMonth,
case when finalfixedchurnflag = 'Involuntary' then '6.Null last day' else mainmovement_raw end as mainmovement,
case when finalfixedchurnflag = 'Involuntary' then 0 else e_numrgus end as e_numrgus_raw
FROM join_so_fixedbase f LEFT JOIN FixedRejoinerMonthPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=CAST(r.Month AS DATE)
)

select Fixed_Month,	Fixed_Account,	f_contactphone1,	f_contactphone2,	f_contactphone3,	ActiveBOM,	ActiveEOM,	B_Date,	B_Tech_Type,	B_MixCode,	B_MixCode_Adj,	B_MixName,	B_MixName_Adj,	B_ProdBBName,	B_ProdTVName,	B_ProdVoName,	BB_RGU_BOM,	TV_RGU_BOM,	VO_RGU_BOM,	B_NumRGUs,	B_bundlecode,	B_bundlename,	B_MRC,	B_OutstAge,	B_MRCAdj,	B_MRCBB,	B_MRCTV,	B_MRCVO,	B_Avg_MRC,	b_Avg_Bill1,	b_Avg_Bill0,	B_MaxStart,	B_TenureDays,	B_FixedTenureSegment,	E_Date,	E_Tech_Type,	E_MixCode,	E_MixCode_Adj,	E_MixName,	E_MixName_Adj,	E_ProdBBName,	E_ProdTVName,	E_ProdVoName,	BB_RGU_EOM,	TV_RGU_EOM,	VO_RGU_EOM,	cast(E_NumRGUs_raw as int) as E_NumRGUs,	E_bundlecode,	E_bundlename,	E_MRC,	E_OutstAge,	E_MRCAdj,	E_MRCBB,	E_MRCTV,	E_MRCVO,	E_Avg_MRC,	E_Avg_Bill1,	E_Avg_Bill0,	E_MaxStart,	E_TenureDays,	E_FixedTenureSegment,	last_rgus,	MRCDiff,	MainMovement,	SpinMovement,	FixedChurnFlag,	FixedChurnTypeFlag,	ChurnTenureDays,	ChurnTenureSegment,	FinalFixedChurnFlag,	Fixed_PRMonth,	Fixed_RejoinerMonth
FROM FullFixedBase_Rejoiners
where fixed_month = date('2022-11-01') 
