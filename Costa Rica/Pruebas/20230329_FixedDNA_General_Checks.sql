------------------------------------------------------------
------------------- ANALISIS fi_oust_age -------------------
------------------------------------------------------------
/*
SELECT  DATE(DT) as DT
        ,sum(IF(fi_outst_age IS NULL,0,1)) AS tiene_mora
FROM (SELECT DISTINCT * FROM "db-analytics-dev"."dna_fixed_cr")
WHERE (act_cust_typ = 'RESIDENCIAL' OR act_cust_typ = 'PROGRAMA HOGARES CONECTADOS')
    AND (act_acct_stat = 'ACTIVO' OR act_acct_stat = 'SUSPENDIDO')
    AND date(dt) BETWEEN DATE('2023-01-01') AND DATE('2023-02-28')
GROUP BY 1 
ORDER BY 1
*/
/*
SELECT  dt
        ,DIFF
        ,COUNT(DISTINCT account) as cuentas
FROM(
SELECT  month
        ,dt
        ,account
        ,mora
        ,mora_anterior
        ,IF(mora is null or mora_anterior is null,0,IF(lst_pymt_dt = dt - interval '1' day,0,mora - mora_anterior)) AS DIFF
FROM (
SELECT  DATE_TRUNC('MONTH',DATE(dt)) AS month
        ,date(dt) as dt
        ,act_acct_cd AS account
        ,fi_outst_age AS mora
        ,LAG (fi_outst_age, 1) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS mora_anterior
        ,LEAD (fi_outst_age, 1) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS mora_posterior
        ,lst_pymt_dt
FROM (SELECT DISTINCT * FROM "db-analytics-dev"."dna_fixed_cr")
WHERE (act_cust_typ = 'RESIDENCIAL' OR act_cust_typ = 'PROGRAMA HOGARES CONECTADOS')
    AND (act_acct_stat = 'ACTIVO' OR act_acct_stat = 'SUSPENDIDO')
    AND date(dt) BETWEEN DATE('2023-01-01') AND DATE('2023-02-28')
))
GROUP BY 1,2
ORDER BY 1,2
*/
------------------------------------------------------------
-------------------- ANALISIS productos --------------------
------------------------------------------------------------


SELECT  dt
        --,DIFF_VO
        --,DIFF_TV
        ,DIFF_BB
        ,count(distinct account) as accounts
FROM (
SELECT  dt
        ,account
        ,RGUs_VO
        ,VO_anterior
        ,(IF(RGUs_VO IS NULL,0,RGUs_VO) - IF(VO_anterior IS NULL,0,VO_anterior)) AS DIFF_VO
        ,RGUs_TV
        ,TV_anterior
        ,(IF(RGUs_TV IS NULL,0,RGUs_TV) - IF(TV_anterior IS NULL,0,TV_anterior)) AS DIFF_TV
        ,RGUs_BB
        ,BB_anterior
        ,(IF(RGUs_BB IS NULL,0,RGUs_BB) - IF(BB_anterior IS NULL,0,BB_anterior)) AS DIFF_BB
FROM(
SELECT  DATE(dt) AS dt
        ,act_acct_cd AS account
        ,RGU_VO AS RGUs_VO
        ,LAG (RGU_VO,1) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS VO_anterior
        ,RGU_TV AS RGUs_TV
        ,LAG (RGU_TV,1) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS TV_anterior
        ,RGU_BB AS RGUs_BB
        ,LAG (RGU_BB,1) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS BB_anterior
FROM (
SELECT DISTINCT *
        ,IF(pd_vo_prod_nm IS NOT NULL and pd_vo_prod_nm <>'',1,0) AS RGU_VO
        ,IF(pd_tv_prod_nm IS NOT NULL and pd_tv_prod_nm <>'',1,0) AS RGU_TV
        ,IF(pd_bb_prod_nm IS NOT NULL and pd_bb_prod_nm <>'',1,0) AS RGU_BB
FROM "db-analytics-dev"."dna_fixed_cr"
WHERE (act_cust_typ = 'RESIDENCIAL' OR act_cust_typ = 'PROGRAMA HOGARES CONECTADOS')
    AND (act_acct_stat = 'ACTIVO' OR act_acct_stat = 'SUSPENDIDO')
    AND date(dt) BETWEEN DATE('2022-12-31') AND DATE('2023-03-01')
)))
GROUP BY 1,2--,3,4
ORDER BY 1,2--,3,4
