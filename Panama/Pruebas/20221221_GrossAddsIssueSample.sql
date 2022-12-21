---------------------------------------------------------------------------------------
------------------------- GROSS ADDS ISSUE SAMPLE -------------------------------------
---------------------------------------------------------------------------------------

WITH 

Cuentas_A_Evaluar as (
select distinct fixedaccount
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
where month=date(dt) and month =date('2022-10-01') and fixedmainmovement ='8.Rejoiner-GrossAdd Gap'
group by 1
order by 1
)

,lag_dna as (
select *, date_trunc('month', date(dt)) month_dt, 
 case when (fi_outst_age is null and (next1_fi_outst_age>90 or next2_fi_outst_age>91) or fi_outst_age>90) then 1 else 0 end as exclude,--
 case when fi_outst_age = (90) or 
        (next1_fi_outst_age = (90) + 1 and date_trunc('month',date(next1_dt) - interval '1' day) = date_trunc('month',date(dt))) or 
        (next2_fi_outst_age = (90) + 2 and date_trunc('month',date(next2_dt) - interval '2' day) = date_trunc('month',date(dt))) or 
        (next3_fi_outst_age = (90) + 3 and date_trunc('month',date(next3_dt) - interval '3' day) = date_trunc('month',date(dt))) or
        (next4_fi_outst_age = (90) + 4 and date_trunc('month',date(next4_dt) - interval '4' day) = date_trunc('month',date(dt)))then 1 else 0 end as inv_churn_flg
from (select *,
        lag(fi_outst_age) over (partition by act_acct_cd order by dt desc) as next1_fi_outst_age,
        lag(fi_outst_age,2) over (partition by act_acct_cd order by dt desc) as next2_fi_outst_age,
        lag(fi_outst_age,3) over (partition by act_acct_cd order by dt desc) as next3_fi_outst_age,
        lag(fi_outst_age,4) over (partition by act_acct_cd order by dt desc) as next4_fi_outst_age,
        lag(dt) over (partition by act_acct_cd order by dt desc) as next1_dt,
        lag(dt,2) over (partition by act_acct_cd order by dt desc) as next2_dt,
        lag(dt,3) over (partition by act_acct_cd order by dt desc) as next3_dt,
        lag(dt,4) over (partition by act_acct_cd order by dt desc) as next4_dt
        from "db-analytics-prod"."fixed_cwp" 
        WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
        --and date(dt) between (DATE('2022-10-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-10-01') + interval '1' MONTH - interval '1' DAY)) 
and date(dt) >= date('2022-01-01')
)
)

,FixedUsefulFields AS(
SELECT load_dt
,ACT_ACCT_CD AS FixedAccount,ACT_CONTACT_PHONE_3 AS CONTACTO,act_cust_strt_dt
,FI_OUTST_AGE,(CAST(CAST(first_value(act_cust_strt_dt) over (partition by act_acct_cd order by dt desc) AS TIMESTAMP) AS DATE)) AS MaxStart, round(FI_TOT_MRC_AMT,0) AS Fixed_MRC
,Case When pd_bb_accs_media = 'FTTH' Then 'FTTH'
        When pd_bb_accs_media = 'HFC' Then 'HFC'
        when pd_TV_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL Then 'FTTH'
        when pd_TV_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL Then 'HFC'
        when pd_VO_accs_media = 'FTTH' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'FTTH'
        when pd_VO_accs_media = 'HFC' AND pd_bb_accs_media  IS NULL AND pd_TV_accs_media IS NULL Then 'HFC'
    ELSE 'COPPER' end as TechFlag
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numBB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN 1 ELSE 0 END AS numTV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' and lower(pd_vo_prod_nm) NOT LIKE'%prepaid%' THEN 1 ELSE 0 END AS numVO
,CASE WHEN pd_bb_prod_cd IS NOT NULL AND CAST(pd_bb_prod_cd AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS BB
,CASE WHEN pd_tv_prod_cd IS NOT NULL AND CAST(pd_tv_prod_cd  AS VARCHAR(50)) <> '' THEN act_acct_cd ELSE NULL END AS TV
,CASE WHEN pd_vo_prod_cd IS NOT NULL AND CAST(pd_vo_prod_cd AS VARCHAR(50)) <> '' and lower(pd_vo_prod_nm) NOT LIKE '%prepaid%' THEN act_acct_cd ELSE NULL END AS VO,
CASE WHEN evt_frst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_frst_sale_chnl = 'Negocios Regionales' or evt_frst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_frst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_frst_sale_chnl = 'TIENDAS' or evt_frst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_frst_sale_chnl = 'D2D' or evt_frst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_frst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_frst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_frst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS FIRST_SALES_CHNL,
CASE WHEN evt_lst_sale_chnl = 'CALL CENTER' THEN 'Tele Sales'
        WHEN evt_lst_sale_chnl = 'Negocios Regionales'  or evt_lst_sale_chnl ='AM REGIONAL'THEN 'Regionales'
        WHEN evt_lst_sale_chnl = 'Dealers' THEN 'Agencias'
        WHEN evt_lst_sale_chnl = 'TIENDAS' or evt_lst_sale_chnl = 'Tiendas'  THEN 'Stores'
        WHEN evt_lst_sale_chnl = 'D2D' or evt_lst_sale_chnl = 'Door 2 Door B2C'  THEN 'D2D'
        WHEN evt_lst_sale_chnl in ( 'Alianzas', 'Promotores', 'Ventas Corporativas') THEN 'Other'
        WHEN evt_lst_sale_chnl = 'Ventas Web' then 'WEB'
         WHEN evt_lst_sale_chnl is null THEN 'No Channel'
         Else NULL 
         END AS LAST_SALES_CHNL,
PD_BB_PROD_CD, pd_tv_prod_cd, PD_VO_PROD_CD, pd_mix_nm,pd_mix_cd,date(dt) as dt,exclude,inv_churn_flg
,first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by date(dt) desc) as Last_Overdue
FROM lag_dna
WHERE PD_MIX_CD<>'0P'AND act_cust_typ_nm = 'Residencial' 
--and date(dt) between (DATE('2022-10-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) AND  (DATE('2022-10-01') + interval '1' MONTH - interval '1' DAY)
and date(dt) >= date('2022-01-01')
)

select *
--distinct date(dt)
--distinct date(act_cust_strt_dt)
from FixedUsefulFields
where FixedAccount in (select fixedaccount from cuentas_a_evaluar)
order by fixedaccount, date(dt)
--order by date(dt)
--order by date(act_cust_strt_dt)
