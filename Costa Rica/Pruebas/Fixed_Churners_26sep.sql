WITH 
------------------------Fixed Useful Fields -------------------------------------------------------------
UsefulFields AS(
SELECT DISTINCT DATE_TRUNC('month',date(FECHA_EXTRACCION)) AS Month,date(FECHA_EXTRACCION) as fecha_extraccion, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, first_value(ACT_ACCT_INST_DT) over(partition by act_acct_cd order by fecha_extraccion) as MinInst, CST_CHRN_DT AS ChurnDate
, DATE_DIFF('day',date(OLDEST_UNPAID_BILL_DT_new2),date(FECHA_EXTRACCION)) AS MORA
, ACT_CONTACT_MAIL_1,round(VO_FI_TOT_MRC_AMT,0) AS mrcVO, round(BB_FI_TOT_MRC_AMT,0) AS mrcBB, round(TV_FI_TOT_MRC_AMT,0) AS mrcTV,round((VO_FI_TOT_MRC_AMT + BB_FI_TOT_MRC_AMT + TV_FI_TOT_MRC_AMT),0) as avgmrc, round(TOT_BILL_AMT,0) AS Bill, ACT_ACCT_SIGN_DT
  ,CASE WHEN pd_vo_prod_id IS NOT NULL and pd_vo_prod_id<>'' THEN 1 ELSE 0 END AS RGU_VO
  ,CASE WHEN pd_tv_prod_cd IS NOT NULL and pd_tv_prod_id<>'' THEN 1 ELSE 0 END AS RGU_TV
  ,CASE WHEN pd_bb_prod_id IS NOT NULL and pd_bb_prod_id<>'' THEN 1 ELSE 0 END AS RGU_BB
  ,CASE WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '3P'
        WHEN (PD_VO_PROD_ID IS NULL or pd_vo_prod_id='')  AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '2P'
        WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND (PD_BB_PROD_ID IS NULL or pd_bb_prod_id='') AND PD_TV_PROD_ID IS NOT NULL and pd_tv_prod_id<>'' THEN '2P'
        WHEN PD_VO_PROD_ID IS NOT NULL and pd_vo_prod_id<>'' AND PD_BB_PROD_ID IS NOT NULL and pd_bb_prod_id<>'' AND (PD_TV_PROD_ID IS NULL or pd_tv_prod_id='') THEN '2P'
ELSE '1P' END AS MIX
from "lla_cco_int_san"."dna_fixed_historic_cr_billfix"  
where date(fecha_extraccion) between (DATE('2022-02-01') + interval '1' MONTH - interval '3' MONTH) AND  (DATE('2022-02-01') + interval '6' MONTH)
)
,CustomerBase_BOM AS(
SELECT *
 ,CASE WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
       WHEN B_Tech_Type IS NULL AND cast(B_RGU_TV AS varchar)='NEXTGEN TV' THEN 'FTTH'
 ELSE 'HFC' END AS B_TechAdj
 ,CASE WHEN B_Tenure <=6 THEN 'Early Tenure'
       WHEN B_Tenure >6 THEN 'Late Tenure'
 ELSE NULL END AS B_FixedTenureSegment
from(SELECT DISTINCT Month, Fecha_Extraccion AS B_DATE, c.act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MinInst as B_MinInst, MIX AS B_MIX,RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs,Tipo_Tecnologia AS B_Tech_Type, MORA AS B_MORA, mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC,BILL AS B_BILL_AMT,ACT_ACCT_SIGN_DT AS B_ACT_ACCT_SIGN_DT
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN '1P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN '2P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS B_Bundle_Type
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS B_BundleName
  ,CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_BOM
  ,CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_BOM
  ,CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_BOM
  ,CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS B_MixCode_Adj
FROM UsefulFields c LEFT JOIN "lla_cco_int_san"."catalogue_tv_internet_cr"  ON PD_BB_PROD_nm=ActivoInternet
WHERE FECHA_EXTRACCION=DATE_TRUNC('month',FECHA_EXTRACCION)  and (mora<=120 or mora is null)
))
,CustomerBase_EOM AS(
select *
 ,CASE WHEN E_Tech_Type IS NOT NULL THEN E_Tech_Type
       WHEN E_Tech_Type IS NULL AND cast(E_RGU_TV AS varchar)='NEXTGEN TV' THEN 'FTTH'
 ELSE 'HFC' END AS E_TechAdj
 ,CASE WHEN E_Tenure <=6 THEN 'Early Tenure'
       WHEN E_Tenure >6 THEN 'Late Tenure'
 ELSE NULL END AS E_FixedTenureSegment
from(SELECT DISTINCT date_add('month',-1,Month) as Month, Fecha_Extraccion as E_Date, c.act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MinInst as E_MinInst, MIX AS E_MIX,RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,ACT_ACCT_SIGN_DT AS E_ACT_ACCT_SIGN_DT
  ,CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN '1P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN '2P'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS E_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS E_BundleName
  ,CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_EOM
  ,CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_EOM
  ,CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_EOM
  ,CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS E_MixCode_Adj
FROM UsefulFields c LEFT JOIN "lla_cco_int_san"."catalogue_tv_internet_cr" ON PD_BB_PROD_nm=ActivoInternet
WHERE FECHA_EXTRACCION=DATE_TRUNC('month',FECHA_EXTRACCION) and (mora<=120 or mora is null)
))
,FixedCustomerBase AS(
    SELECT DISTINCT
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
   END AS Fixed_Month,
     CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MinInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_FixedTenureSegment, B_MORA, B_VO_MRC, B_BB_MRC, B_TV_MRC, B_AVG_MRC, B_BILL_AMT,B_ACT_ACCT_SIGN_DT,BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,B_MixCode_Adj,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MinInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_FixedTenureSegment, E_MORA, E_VO_MRC, E_BB_MRC, E_TV_MRC, E_AVG_MRC, E_BILL_AMT,E_ACT_ACCT_SIGN_DT,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM,E_MixCode_Adj
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)
-----------------------------Main Movements------------------------------------------------------------
,MAINMOVEMENTBASE AS(
 SELECT f.*
 ,CASE WHEN (E_NumRGUs - B_NumRGUs)=0 THEN 'Same RGUs'
       WHEN (E_NumRGUs - B_NumRGUs)>0 THEN 'Upsell'
       WHEN (E_NumRGUs - B_NumRGUs)<0 then 'Downsell'
       WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC('month',E_ACT_ACCT_SIGN_DT) <> Fixed_Month) THEN 'Come Back to Life'
       WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC('month',E_ACT_ACCT_SIGN_DT) = Fixed_Month) THEN 'New Customer'
       WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN 'Loss'
 END AS MainMovement
 ,CASE WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC('month',E_MinInst) = date('2022-06-01') THEN 'June Gross-Ads'
       WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC('month',E_MinInst) <> date('2022-06-01') THEN 'ComeBackToLife/Rejoiners Gross-Ads'
 ELSE NULL END AS GainMovement
 ,coalesce(E_RGU_BB - B_RGU_BB,0) as DIF_RGU_BB ,coalesce(E_RGU_TV - B_RGU_TV,0) as DIF_RGU_TV ,coalesce(E_RGU_VO - B_RGU_VO,0) as DIF_RGU_VO,(E_NumRGUs - B_NumRGUs) as DIF_TOTAL_RGU
FROM FixedCustomerBase f
)
,SPINMOVEMENTBASE AS (
SELECT b.*,
 CASE WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN '1. Up-spin' 
      WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN '2. Down-spin' 
 ELSE '3. No Spin' END AS SpinMovement
FROM MAINMOVEMENTBASE b
)
------------------------------------Fixed Churn Flags----------------------------------------------
--------------------Voluntary
,ServiceOrders AS (
SELECT * FROM "lla_cco_int_san"."so_fixed_historic_cr"
)
,MAX_SO_CHURN AS(
SELECT DISTINCT reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0')) as contratoso
,DATE_TRUNC('month',FECHA_PEDIDO) as DeinstallationMonth
--, DATE_TRUNC('month',MAX(FECHA_PEDIDO)) as DeinstallationMonth--, MAX(FECHA_PEDIDO) AS FECHA_CHURN
,first_value(fecha_pedido) over(partition by nombre_contrato,DATE_TRUNC('month',FECHA_PEDIDO) order by fecha_pedido desc) as fecha_churn
,first_value(submotivo) over(partition by nombre_contrato,DATE_TRUNC('month',FECHA_PEDIDO) order by fecha_pedido desc) as submotivo22
FROM ServiceOrders WHERE TIPO_ORDEN = 'DESINSTALACION' AND ESTADO NOT IN ('CANCELADA','ANULADA') AND FECHA_PEDIDO IS NOT NULL
 --and cast(nombre_contrato as varchar) NOT LIKE '%E%' -- temporal
--GROUP BY 1
)
,CHURNERSSO AS(
SELECT DISTINCT reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0')) as contratoso,DATE_TRUNC('month',FECHA_PEDIDO) as DeinstallationMonth,Fecha_Pedido as DeinstallationDate,
CASE WHEN m.submotivo22='MOROSIDAD' THEN 'Involuntary'
       WHEN m.submotivo22 <> 'MOROSIDAD' AND m.submotivo22 <> 'DESINSTALACION POR TX'
       AND m.submotivo22 NOT LIKE '%TRAMITE%' AND m.submotivo22 NOT LIKE '%MIGRACION%' 
       AND m.submotivo22 NOT LIKE '%CORPORATIVO%' AND m.submotivo22 <> 'BAJA VOLUNTARIA B2B'
       AND m.submotivo22 <> 'HOTELERIA E INMOBILIARIO'
       THEN 'Voluntary' END AS Submotivo
FROM ServiceOrders t INNER JOIN MAX_SO_CHURN m on reverse(rpad(substr(reverse(nombre_contrato),1,10),10,'0'))= m.contratoso and fecha_Pedido = fecha_churn
WHERE TIPO_ORDEN = 'DESINSTALACION' AND ESTADO NOT IN ('CANCELADA','ANULADA') AND FECHA_PEDIDO IS NOT NULL
--and cast(nombre_contrato as varchar) NOT LIKE '%E%' --temporal
)
,MaximaFecha as(
select distinct reverse(rpad(substr(reverse(cast(act_acct_cd as varchar)),1,10),10,'0')) as act_acct_cd, max(fecha_extraccion) as MaxFecha 
FROM "lla_cco_int_san"."dna_fixed_historic_cr"  group by 1
)
,ChurnersJoin as(
select Distinct f.Fecha_Extraccion,f.act_acct_cd,Submotivo,DeinstallationMonth,DeinstallationDate,MaxFecha 
FROM "lla_cco_int_san"."dna_fixed_historic_cr" f
left join churnersso c on contratoso=reverse(rpad(substr(reverse(cast(f.act_acct_cd as varchar)),1,10),10,'0'))
and date_trunc('month',fecha_extraccion)=DeinstallationMonth
left join MaximaFecha m on reverse(rpad(substr(reverse(cast(f.act_acct_cd as varchar)),1,10),10,'0'))=reverse(rpad(substr(reverse(cast(m.act_acct_cd as varchar)),1,10),10,'0'))
)
,MaxFechaJoin as(
select distinct dxmonth as Month,act_acct_Cd as Account,case when FixedChurnTypeFlag_prel='Voluntary' then '1.Voluntary Churner' else null end as FixedChurnType
from(select Fecha_extraccion,DeinstallationMonth as DxMonth,reverse(rpad(substr(reverse(cast(act_acct_cd as varchar)),1,10),10,'0')) as act_acct_cd
,CASE WHEN date_diff('month',DeinstallationMonth,MaxFecha)<=1 THEN Submotivo
ELSE NULL END AS FixedChurnTypeFlag_prel
FROM Churnersjoin WHERE Submotivo IS NOT NULL)
)
-----------Involuntary
,FIRSTCUSTRECORD AS (
SELECT date_add('month',1,DATE_TRUNC('MONTH',DATE(fecha_extraccion))) AS MES, act_acct_cd AS Account,date(fecha_extraccion) as FirstCustRecord
FROM UsefulFields 
WHERE date(fecha_extraccion)=date_trunc('MONTH',DATE(fecha_extraccion)) + interval '1' month - interval '1' day
)
,LastCustRecord as(
SELECT DATE_TRUNC('MONTH', DATE(fecha_extraccion)) AS MES, act_acct_cd AS Account
,first_value(date(fecha_extraccion)) over(partition by act_acct_cd, DATE_TRUNC('MONTH',DATE(fecha_extraccion)) order by fecha_extraccion desc) as LastCustRecord
FROM UsefulFields 
)
,NO_OVERDUE AS(
SELECT DISTINCT DATE_TRUNC('MONTH', Date_add('MONTH',1, DATE(fecha_extraccion))) AS MES, act_acct_cd AS Account, mora
FROM UsefulFields t INNER JOIN FIRSTCUSTRECORD  r ON r.account = t.act_acct_cd
WHERE CAST(mora as INT) < 90 and date(t.fecha_extraccion) = r.FirstCustRecord
GROUP BY 1, 2, 3
)
,OVERDUELASTDAY AS(
SELECT DISTINCT DATE_TRUNC('MONTH', DATE(fecha_extraccion)) AS MES, act_acct_cd AS Account, mora,
(date_diff('DAY', MinInst,DATE(fecha_extraccion))) as ChurnTenureDays
FROM UsefulFields t INNER JOIN LastCustRecord r ON r.account = t.act_acct_cd and date(t.fecha_extraccion)=r.LastCustRecord
WHERE CAST(mora AS INTEGER) >= 90
GROUP BY 1, 2, 3, 4
)

,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month
 , reverse(rpad(substr(reverse(cast(n.account as varchar)),1,10),10,'0')) as account
 ,n.account as account_prev
 ,'2.Involuntary Churner' as fixedchurntype
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,AllChurners AS(
SELECT DISTINCT Month,Account,FixedChurnType
from (SELECT Month,Account,FixedChurnType from maxfechajoin a 
      UNION ALL
      SELECT Month,Account,FixedChurnType  from InvoluntaryNetChurners b)
)
,ChurnersFixedTable as(
select f.*,FixedChurnType,reverse(rpad(substr(reverse(cast(fixed_account as varchar)),1,10),10,'0'))
FROM SPINMOVEMENTBASE f left join AllChurners b
on Fixed_Month=date_trunc('month',Month) and reverse(rpad(substr(reverse(cast(fixed_account as varchar)),1,10),10,'0'))= cast(b.account as varchar)
)

------------------------------------Rejoiners--------------------------------------------------------------
,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH',1,Fixed_Month) AS RejoinerMonth
FROM FixedCustomerBase 
WHERE ActiveBOM=1 AND ActiveEOM=0
)
,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
-- Variabilizar
,CASE WHEN RejoinerMonth>=date('2022-06-01') AND RejoinerMonth<=DATE_ADD('month',1,date('2022-06-01')) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)
,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,date('2022-06-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1 AND Fixed_PR=1 AND Fixed_Month<>date('2022-06-01')
GROUP BY 1,2,3,4
)
,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement='Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM ChurnersFixedTable f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=date(r.Month)
)
,FinalTable as(
SELECT *,CASE
WHEN FixedChurnType is not null THEN b_NumRGUs
WHEN MainMovement='Downsell' THEN (B_NumRGUs - coalesce(E_NumRGUs,0))
ELSE NULL END AS RGU_Churn,
CONCAT(coalesce(B_VO_nm,''),coalesce(B_TV_nm,''),coalesce(B_BB_nm,'')) AS B_PLAN,CONCAT(coalesce(E_VO_nm,''),coalesce(E_TV_nm,''),coalesce(E_BB_nm,'')) AS E_PLAN
FROM FullFixedBase_Rejoiners
)

select distinct fixed_month,fixedchurntype,count(distinct fixed_account) as num_accounts,sum(b_numrgus) as num_RGUs
from finaltable
where activebom=1 and fixedchurntype in ('1.Voluntary Churner','2.Involuntary Churner')
group by 1,2 order by 2,1

--, PRUEBA_invol AS (
--SELECT * 
--FROM FinalTable A left join CHURNERSSO B on --A.fixed_month = B.DeinstallationMonth and 
--reverse(rpad(substr(reverse(cast(A.fixed_account as varchar)),1,10),10,'0')) = B.contratoso
--)

--select distinct fixed_month,DeinstallationMonth,fixedchurntype,submotivo,
--count(distinct fixed_account) as NUM_ACCOUNTS,sum(b_numrgus) as SUM_RGUs
--from PRUEBA_invol c
--where activebom=1 and fixedchurntype = '2.Involuntary Churner' --and year(DeinstallationMonth)=2022 and DeinstallationMonth>=fixed_month
--and fixedchurntype in('2.Involuntary Churner','1.Voluntary Churner')
--group by 1,2,3,4 order by 1,2,3,4
