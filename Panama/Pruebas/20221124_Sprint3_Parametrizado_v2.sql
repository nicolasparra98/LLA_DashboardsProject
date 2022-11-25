-----------------------------------------------------------------------------------------
------------------------- SPRINT 3 PARAMETRIZADO - V2 -----------------------------------
-----------------------------------------------------------------------------------------

WITH

parameters as(
select 
date_trunc('month',date('2022-10-01')) as input_month
)

,FMC_Table AS
( SELECT * FROM
"lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where Month=date(dt) and month = (SELECT input_month FROM PARAMETERS)
)

-----New Customers-----

,previous_months_dna as (
SELECT  distinct DATE_TRUNC('MONTH',CAST(DT AS DATE)) AS month
        ,act_acct_cd
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
        --AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
        AND date_trunc('month',date(dt)) between ((SELECT input_month FROM PARAMETERS) - interval '3' month) and ((SELECT input_month FROM PARAMETERS) - interval '1' month)
GROUP BY 1,2
ORDER BY 1,2
)

,NEW_CUSTOMERS as (
Select 
act_acct_cd,date(dt) as dt, DATE_TRUNC('MONTH',CAST(dt AS DATE)) AS month_load,DATE_TRUNC('MONTH',CAST(act_cust_strt_dt AS DATE)) AS month_start,CAST(SUBSTR(pd_mix_cd,1,1) AS INT) AS n_rgu, max(act_acct_inst_dt) as act_acct_inst_dt ,max(act_cust_strt_dt) as act_cust_strt_dt,  
    --DATE_DIFF ('DAY',CAST (max(act_cust_strt_dt) AS DATE),CAST (max(act_acct_inst_dt) AS DATE)) as Installation_lapse, 
    1 as NEW_CUSTOMER,
pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
from "db-analytics-prod"."fixed_cwp"
WHERE act_cust_typ_nm = 'Residencial'
AND act_acct_cd  not in (select distinct act_acct_cd from previous_months_dna)
and DATE_TRUNC('month',CAST(dt AS DATE)) = (SELECT input_month FROM PARAMETERS)
--AND DATE_TRUNC('month',CAST(dt AS DATE)) = DATE_TRUNC('month',CAST(act_cust_strt_dt AS DATE))
GROUP BY act_acct_cd, 2, DATE_TRUNC('MONTH',CAST(dt AS DATE)),CAST(act_cust_strt_dt AS DATE),
CAST(SUBSTR(pd_mix_cd,1,1) AS INT),1, pd_bb_accs_media,pd_tv_accs_media,pd_vo_accs_media
)

,New_Customers_FLAG as(
SELECT f.*, --a.installation_lapse, 
a.new_customer, 
CASE when f.FIRST_SALES_CHNL_BOM is not null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
when f.FIRST_SALES_CHNL_BOM is null and f.FIRST_SALES_CHNL_EOM is not null then f.FIRST_SALES_CHNL_EOM
WHEN  f.FIRST_SALES_CHNL_EOM is null and f.FIRST_SALES_CHNL_BOM is not null then f.FIRST_SALES_CHNL_BOM
END as SALES_CHANNEL,
CASE WHEN a.act_acct_cd is not null then 1 else 0 end as monthsale_flag
 FROM FMC_TABLE AS f left join NEW_CUSTOMERS AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_load
)
------------ INTERACTIONS ---------
,clean_interaction_time as (
select *
FROM "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME  as VARCHAR) != ' ')
    AND(INTERACTION_START_TIME IS NOT NULL)
    and DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) between ((SELECT input_month FROM PARAMETERS)) and ((SELECT input_month FROM PARAMETERS) + interval '1' month)
    --AND DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) >= DATE ('2021-09-01')
    --AND INTERACTION_ID NOT LIKE '%-%' Filtro Siebel
)

,Interactions_inicial as (
    select *,
    CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS INTERACTION_DATE, DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month,
    CASE WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info Like '%retenci%n%'  THEN 'retention_claim'	 
        WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info   like '%restringido%' then 'service_restriction_claim'		
        when interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info  Like'%instalacion%'  then 'installation_claim'
        when interaction_purpose_descrip = 'CLAIM' and (
            interaction_disposition_info like '%afiliacion%factura%' or
            interaction_disposition_info like '%cliente%desc%' or
            interaction_disposition_info like '%consulta%cuentas%' or
            interaction_disposition_info like '%consulta%productos%' or
            interaction_disposition_info like '%consumo%' or
            interaction_disposition_info like '%info%cuenta%productos%' or
            interaction_disposition_info like '%informacion%general%' or
            interaction_disposition_info like '%pagar%on%line%' or
            interaction_disposition_info like '%saldo%' or
            interaction_disposition_info like '%actualizacion%datos%' or
            interaction_disposition_info like '%traslado%linea%' or
            interaction_disposition_info like '%transfe%cta%'
            ) THEN 'account_info_or_balance_claim'
        when interaction_purpose_descrip = 'CLAIM' and (
            interaction_disposition_info like '%cargo%' or
            interaction_disposition_info like '%credito%' or
            interaction_disposition_info like '%facturaci%n%' or
            interaction_disposition_info like '%pago%' or
            interaction_disposition_info like '%prorrateo%' or
            interaction_disposition_info like '%alto%consumo%' or
            interaction_disposition_info like '%investigacion%interna%' or
            interaction_disposition_info like '%cambio%descuento%'
            ) THEN 'billing_claim'	
        when interaction_purpose_descrip = 'CLAIM' and (
            interaction_disposition_info like '%venta%' or
            interaction_disposition_info like '%publicidad%' or
            interaction_disposition_info like '%queja%promo%precio%' or
            interaction_disposition_info like '%promo%' or
            interaction_disposition_info like '%promo%' or
            interaction_disposition_info like '%cambio%de%precio%' or  
            interaction_disposition_info like '%activacion%producto%' or
            interaction_disposition_info like '%productos%servicios%-internet%' or
            interaction_disposition_info like '%productos%servicios%suplementarios%' or
            interaction_disposition_info like '%paytv%-tv%digital%hd%'
        ) THEN 'sales_claim'	
        when interaction_purpose_descrip = 'CLAIM' and (
            interaction_disposition_info like '%queja%mala%atencion%' or
            interaction_disposition_info like '%dunning%' or
            interaction_disposition_info like '%consulta%reclamo%' or
            interaction_disposition_info like '%apertura%reclamo%' or
            interaction_disposition_info like '%horario%tienda%' or
            interaction_disposition_info like '%consulta%descuento%' or
            interaction_disposition_info like '%cuenta%apertura%reclamo%' or
            interaction_disposition_info like '%actualizar%apc%' or
            interaction_disposition_info like '%felicita%' 
        ) THEN 'customer_service_claim'
        WHEN interaction_purpose_descrip = 'CLAIM' AND (
            interaction_disposition_info like '% da%no%' or
            interaction_disposition_info like '%-da%no%' or
            interaction_disposition_info like '%servicios%-da%os%' or
            interaction_disposition_info like '%datos%internet%' or
            interaction_disposition_info like '%equipo%recuperado%' or
            interaction_disposition_info like '%escalami%niv%' or
            interaction_disposition_info like '%queja%internet%' or
            interaction_disposition_info like '%queja%linea%' or
            interaction_disposition_info like '%queja%tv%' or
            interaction_disposition_info like '%reclamos%tv%' or
            interaction_disposition_info like '%serv%func%' or
            interaction_disposition_info like '%soporte%internet%' or
            interaction_disposition_info like '%soporte%linea%' or
            interaction_disposition_info like '%tecn%' or
            interaction_disposition_info like '%no%casa%internet%' or
            interaction_disposition_info like '%no%energia%internet%' or
            interaction_disposition_info like '%soporte%' or
            interaction_disposition_info like '%intermiten%' or
            interaction_disposition_info like '%masivo%'
            ) THEN 'technical_claim'	
        WHEN interaction_purpose_descrip = 'CLAIM' then 'other_claims'							
        WHEN interaction_purpose_descrip = 'TICKET' then 'tech_ticket'							
        WHEN interaction_purpose_descrip = 'TRUCKROLL' then 'tech_truckroll'							
        END AS interact_category
FROM (select *,
    CASE    WHEN interaction_purpose_descrip = 'CLAIM' THEN replace(concat(lower(COALESCE(other_interaction_info4,' ')),'-',lower(COALESCE(other_interaction_info5,' '))),'  ','') 
            WHEN (interaction_purpose_descrip = 'TICKET' or interaction_purpose_descrip = 'TRUCKROLL') then replace(concat(lower(COALESCE(other_interaction_info8,' ')),'-',lower(COALESCE(other_interaction_info9,' '))),'  ','') ELSE NULL END AS interaction_disposition_info
    from clean_interaction_time
    WHERE interaction_id is not null 
        )
)

,interactions as(
select *, case  when interact_category = 'billing_claim' then 'Billing'
                when interact_category = 'account_info_or_balance_claim' then 'Account Info'
                when interact_category = 'retention_claim' then 'Retention'
                when interact_category in ('installation_claim','tech_ticket','tech_truckroll','technical_claim') then 'Technical'
                else 'Others' end as INTERACTION_TYPE
from interactions_inicial
)
----------------------- Soft DX A9 and Never Pay A13----------------------
,union_dna as (
    select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,date_trunc('month',date(dt)) as Month_load,fi_bill_dt_m0,fi_bill_dt_m1,fi_bill_due_dt_m1,fi_bill_due_dt_m0,fi_bill_dt_m2,fi_bill_due_dt_m2
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
    and date_trunc('month',date(act_acct_inst_dt)) between ((SELECT input_month FROM PARAMETERS) - interval '2' month) and ((SELECT input_month FROM PARAMETERS) + interval '1' month)
)
,monthly_inst_accounts as (
select distinct act_acct_cd,DATE_TRUNC('month',date(act_acct_inst_dt)) as InstMonth
from union_dna
WHERE act_cust_typ_nm = 'Residencial' and DATE_TRUNC('month',date(act_acct_inst_dt)) = month_load
)
,first_bill as(
SELECT distinct act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill,date_trunc('month',first_inst_dt) as instmonth
 FROM(select act_acct_cd,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
    from (select act_acct_cd, fi_outst_age, date(dt) as dt,act_acct_inst_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
        and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
        AND date(dt) between ((DATE_TRUNC('month',date(act_cust_strt_dt))) - interval '12' month) and ((DATE_TRUNC('month',date(act_cust_strt_dt))) + interval '6' month) )
  where oldest_unpaid_bill_dt <> '1900-01-01' )
 group by act_acct_cd,3
)

,max_overdue_first_bill as (
select act_acct_cd, DATE_TRUNC('month',date(min(first_inst_dt))) as Month_Inst,
min(date(first_oldest_unpaid_bill_dt)) as first_oldest_unpaid_bill_dt,
min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
max(fi_outst_age) as max_fi_outst_age, 
max(fi_overdue_age) as max_fi_overdue_age,
max(date(dt)) as max_dt,
case when max(cast(fi_outst_age as int))>=(90) then 1 else 0 end as hard_dx_flg
FROM (select act_acct_cd,
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
    FIRST_VALUE(date(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt,
    fi_outst_age, date(dt) as dt, pd_mix_cd,fi_overdue_age
    FROM ( select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
        ,Case when fi_bill_dt_m0 is not null then cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m0),  date(fi_bill_due_dt_m0))
   when fi_bill_dt_m1 is not null then cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m1),  date(fi_bill_due_dt_m1))
   else cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m2),  date(fi_bill_due_dt_m2)) end as fi_overdue_age
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
         and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
         AND date(dt) between (DATE_TRUNC('month',date(act_acct_inst_dt))) and ((DATE_TRUNC('month',date(act_acct_inst_dt))) + interval '5' month) )
    where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill) )
group by act_acct_cd
)
,sft_hard_dx as(
select *, 
date_add('day',(46),first_oldest_unpaid_bill_dt) as threshold_pay_date,
case when (max_fi_outst_age>=46 and Month_Inst <date('2022-05-01')) or(max_fi_overdue_age>=5 and Month_Inst>=date('2022-05-01')) then 1 else 0 end as soft_dx_flg,
case when date_add('day',(46),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as soft_dx_window_completed,
case when date_add('day',(90),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as never_paid_window_completed,
current_date as current_date_analysis
from max_overdue_first_bill
)
,Join_dx_new_customers as (
SELECT a.month_start, a.act_acct_cd, soft_dx_flg as soft_dx, hard_dx_flg as hard_dx
FROM New_customers AS a
LEFT JOIN sft_hard_dx AS b ON a.act_acct_cd = b.act_acct_cd
)
,FLAG_SOFT_HARD_DX as(
SELECT f.*, soft_dx, hard_dx,
CASE WHEN soft_dx = 1 THen 1 ELSE null End as STRAIGHT_SOFT_DX_FLAG,
CASE WHEN HARD_DX = 1 THen 1 ELSE null End as NEVER_PAID_FLAG
FROM NEW_CUSTOMERS_FLAG AS f left join Join_dx_new_customers AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_start
)
-------------- LATE INSTALLATIONS B1 ------

,Service_Orders AS(
SELECT  *
        ,date_trunc('Month', DATE(order_start_date)) as month
        ,DATE(order_start_date) AS StartDate
        ,DATE(completed_date) AS EndDate
        ,date_diff ('DAY',DATE(order_start_date),DATE(completed_date)) AS Installation_lapse
FROM "db-stage-dev"."so_hdr_cwp"
WHERE order_type = 'INSTALLATION' AND ACCOUNT_TYPE='R' AND ORDER_STATUS='COMPLETED'
        AND DATE_TRUNC('MONTH',CAST(order_start_date AS DATE)) = (SELECT input_month FROM PARAMETERS)
)

,Late_installation_flag as(
Select a.*, 
CASE WHEN b.Installation_lapse > 5 then 1 else 0 END as late_inst_flag
From FLAG_SOFT_HARD_DX a left join service_orders b on a.month=b.month and cast(a.finalaccount AS VARCHAR)=cast(b.account_id AS VARCHAR)
)
,Join_newcustomers_interactions as (
SELECT
b.account_id,
CASE WHEN account_id IS NOT NULL then 1 else 0 END as early_interaction_flag
FROM NEW_CUSTOMERS AS a
LEFT JOIN interactions AS b
ON a.act_acct_cd = b.account_id
WHERE DATE_DIFF ('DAY',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=21
AND interaction_type ='Technical'
GROUP BY account_id
)
------ EARLY INTERACTION B4 -----
,NEW_CUSTOMER_INTERACTIONS_INFO as (
SELECT
a.act_acct_cd,b.account_id,a.month_start,
CASE WHEN account_id IS NOT NULL then 1 else 0 END as early_interaction_flag
FROM NEW_CUSTOMERS AS a
LEFT JOIN interactions AS b
ON a.act_acct_cd = b.account_id
WHERE DATE_DIFF ('DAY',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=21
AND interaction_type ='Technical'
GROUP BY 1,2,3
)
,EARLY_INT_FLAG as(
SELECT f.*, early_interaction_flag
FROM Late_Installation_Flag AS f left join NEW_CUSTOMER_INTERACTIONS_INFO AS a
ON f.finalaccount = a.act_acct_cd and f.month = a.month_start)
----- EARLY TICKET B9 ----
,EARLY_TICKET_INFO as (
SELECT
f.account_id, f.month,
CASE WHEN f.account_id IS NOT NULL then 1 else NULL END as early_ticket_flag
FROM NEW_CUSTOMERS AS e
inner JOIN interactions AS f
ON e.act_acct_cd = f.account_id
WHERE DATE_DIFF ('week',CAST (act_acct_inst_dt AS DATE),CAST (interaction_date AS DATE)) <=7
AND INTERACTION_PURPOSE_DESCRIP = 'TICKET'
GROUP BY account_id, month
)
,EARLY_TKT_FLAG as (
SELECT f.*, early_ticket_flag
FROM EARLY_INT_FLAG AS f left join EARLY_TICKET_INFO AS a
ON f.finalaccount = a.account_id and f.month = a.month
)
-------- Billing CLaims C6 -----
,STOCK_KEY as (
Select A.* ,CONCAT(FinalAccount, SUBSTR(CAST(month AS varchar),1,7)) AS key_Stock
FROM EARLY_TKT_FLAG as A
order by finalaccount
)
,customers_billing_claims as (
SELECT  CONCAT(ACCOUNT_ID, SUBSTR(CAST(DATE_TRUNC('Month',INTERACTION_DATE) AS VARCHAR),1,7)) as Key_bill_claims
FROM interactions
WHERE INTERACTION_TYPE = 'Billing'
GROUP BY CONCAT(ACCOUNT_ID, SUBSTR(CAST(DATE_TRUNC('Month',INTERACTION_DATE) AS VARCHAR),1,7))
)
,Billing_claims_FLAG as (
SELECT
a.*,
CASE WHEN Key_bill_claims IS NOT NULL then 1 else 0 END as Bill_claim_flag
FROM STOCK_KEY AS a
LEFT JOIN customers_billing_claims AS b
ON a.key_stock = b.Key_bill_claims
)
----------- MRC Changes ----------
,MRC_changes as (
SELECT  CONCAT(act_acct_cd, SUBSTR(CAST(DATE_TRUNC('Month',date(DT)) AS VARCHAR),1,7)) as Key_MRC_changes
,((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev)  AS MRC_Change
FROM  "db-analytics-prod"."fixed_cwp"
WHERE pd_vo_prod_nm_prev = pd_vo_prod_nm
AND pd_bb_prod_nm_prev = pd_BB_prod_nm
AND pd_tv_prod_nm_prev = pd_tv_prod_nm
and date_trunc('month',date(dt)) between ((SELECT input_month FROM PARAMETERS) - interval '2' month) and ((SELECT input_month FROM PARAMETERS) + interval '1' month)
group by CONCAT(act_acct_cd, SUBSTR(CAST(DATE_TRUNC('Month',date(DT)) AS VARCHAR),1,7)),((fi_tot_mrc_amt-fi_tot_mrc_amt_prev)/fi_tot_mrc_amt_prev) 
)
,Join_MRC_change as (
SELECT
a.*,
b.MRC_CHANGE
FROM Billing_claims_FLAG AS a
LEFT JOIN MRC_changes AS b
ON a.key_stock = b.Key_MRC_changes
)
,Change_MRC_Flag AS (
SELECT *
 ,CASE WHEN MRC_change > 0.05 or MRC_change< -0.05 then 1 else 0 END as MRC_change_flag
 ,case when mrc_change is not null then finalaccount else null end as NoPlanChange
FROM Join_MRC_change
)
----------------- Mounting Bills ------------
,Overdue_records as (
select 
    date_trunc('Month',cast (dt as date)) as Month,
    act_acct_cd, 
    case when fi_outst_age is null then -1 else fi_outst_age end as fi_outst_age,
    case when fi_outst_age=60 --AND cast(fi_bill_amt_m0 as double) is not null and cast(fi_bill_amt_m0 as double) >0 
    then 1 else 0 end as day_60,
    first_value(case when fi_outst_age is null then -1 else fi_outst_age end) IGNORE NULLS over(partition by date_trunc('Month',cast (dt as date)),
    act_acct_cd order by dt desc) as last_overdue_record,
    first_value(case when fi_outst_age is null then -1 else fi_outst_age end) IGNORE NULLS over(partition by date_trunc('Month',cast (dt as date)),
    act_acct_cd order by dt) as first_overdue_record
from "db-analytics-prod"."fixed_cwp"
where act_cust_typ_nm = 'Residencial' and cast(dt as date) between
date_trunc('MONTH', cast(dt as date)) and date_add('MONTH', 1, date_trunc('MONTH', cast(dt as date)))
)
,Grouped_Overdue_records as (
select Month, act_acct_cd,
max(fi_outst_age) as max_overdue,
max(day_60) as mounting_bill_flag,
max(last_overdue_record) as last_overdue_record,
max(first_overdue_record) as first_overdue_record
from Overdue_records
GROUP BY Month, act_acct_cd
)
,Mounting_bills_Flag as(
select f.*, b.Mounting_bill_flag
from CHANGE_MRC_FLAG as f left join Grouped_Overdue_records as B
ON f.finalaccount = b.act_acct_cd AND f.Month = b.Month
)
,ALL_FLAGS as(
SELECT * FROM Mounting_bills_Flag)
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX  RESULTS  XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX           XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-------------- FLAGS TABLE -----------

,FullTable_KPIsFlags AS(
Select *, case when Monthsale_flag = 1 then concat(cast(Monthsale_flag as varchar), FixedAccount) else NULL end as F_SalesFlag, 
   case when STRAIGHT_SOFT_DX_FLAG = 1 and New_customer =1 then concat(cast(STRAIGHT_SOFT_DX_FLAG as varchar), FixedAccount) else NULL end as F_SoftDxFlag, 
    case when Never_Paid_Flag = 1 then concat(cast(Never_Paid_Flag as varchar), FixedAccount) else NULL end as F_NeverPaidFlag,
    case when Late_inst_flag = 1 and New_customer =1 then concat(cast(late_inst_flag as varchar), FixedAccount) else NULL end as F_LongInstallFlag,
    case when early_interaction_flag = 1 and New_customer =1 then concat(cast(early_interaction_flag as varchar), FixedAccount) else NULL end as F_EarlyInteractionFlag,
    case when early_ticket_flag = 1 and New_customer =1 then concat(cast(early_ticket_flag as varchar), FixedAccount) else NULL end as F_EarlyTicketFlag,
    case when Bill_claim_flag = 1 then concat(cast(Bill_claim_flag as varchar), FixedAccount) else NULL end as F_BillClaim,
    case when MRC_change_flag = 1 then concat(cast(MRC_change_flag as varchar), FixedAccount) else NULL end as F_MRCChange,
    case when Mounting_bill_flag = 1 then concat(cast(mounting_bill_flag as varchar), Fixedaccount) else NULL end as F_MountingBillFlag
From All_Flags
)
,SalesChannel_SO AS(
SELECT DISTINCT Month,Channel_desc,account_id
,CASE WHEN Channel_desc IN ('Provincia de Chiriqui','PROM','VTASE','PHs1','Busitos','Alianza','Coronado','Ventas Externas/ADSL','PHs 2') OR Channel_desc LIKE '%PROM%' OR Channel_desc LIKE '%VTASE%' OR Channel_desc LIKE '%Busitos%' OR Channel_desc LIKE '%Alianza%' THEN 'D2D (Own Sales force)'
    WHEN Channel_desc IN('Dinamo','Oficinista','Distribuidora Arandele','Orde Technology','SLAND','SI Panamá') THEN 'D2D (Outsourcing)'
    WHEN Channel_desc IN('Vendedores','Metro Mall','WESTLAND MALL','TELEMART AGUADULCE') THEN 'Retail (Own Stores)'
    WHEN Channel_desc IN(/*'Telefono',*/'123 Outbound','Gestión') OR Channel_desc LIKE '%Gestión%' OR Channel_desc LIKE '%Gestion%' THEN 'Outbound – TeleSales'
    WHEN Channel_desc IN('Centro de Retencion','Centro de Llamadas','Call Cnter MULTICALL') THEN 'Inbound – TeleSales'
    WHEN Channel_desc IN('Nestrix','Tienda OnLine','Live Person','Telefono') THEN 'Digital'
    WHEN Channel_desc IN('Panafoto Dorado','Agencia') OR Channel_desc LIKE '%Agencia%' OR Channel_desc LIKE '%AGENCIA%' THEN 'Retail (Distributer-Dealer)'
    WHEN Channel_desc IN('CIS+ GUI','Solo para uso de IT','Apuntate',' CU2Si','RC0E Collection','Carta','Proyecto','DE=Demo','Recarga saldo','Port Postventa','Feria','Administracion','Postventa-verif.orde','No Factibilidad','Orden a construir','Inversiones AP','Promotor','VIVI MAS') OR Channel_desc LIKE '%Feria%' THEN 'Not a Sales Channel'
END AS Sales_Channel_SO
FROM  ( SELECT DISTINCT date_trunc('Month', date(completed_date)) AS Month, account_id,first_value(channel_desc) over   (partition by account_id order by order_start_date) as channel_desc
        FROM "db-stage-dev"."so_hdr_cwp" WHERE order_type ='INSTALLATION' and DATE_TRUNC('month',CAST(completed_date AS DATE)) = (SELECT input_month FROM PARAMETERS))
)
,FullTable_Adj AS(
SELECT DISTINCT  f.*,Sales_Channel_SO
FROM FullTable_KPIsFlags f LEFT JOIN SalesChannel_SO s ON f.fixedaccount=cast(s.account_id as varchar)
)

------ RESULTS QUERY -----------------------
select Month
, B_Final_TechFlag, B_FMCSegment, E_Final_TechFlag, E_FMCSegment, b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure
, count(distinct fixedaccount) as activebase, sum(monthsale_flag) as Sales, sum(STRAIGHT_SOFT_DX_FLAG) as Soft_Dx, sum (Never_Paid_Flag) as NeverPaid, sum(late_inst_flag) as Long_installs,sum(early_interaction_flag) as Early_Issues, sum(early_ticket_flag) as Early_ticket, count(distinct F_SalesFlag) as Unique_Sales, count(distinct F_SoftDxFlag) as Unique_SoftDx,
    count(distinct F_NeverPaidFlag) as Unique_NeverPaid,--
    count(distinct F_LongInstallFlag) as Unique_LongInstall,--
    count(distinct F_EarlyInteractionFlag) as Unique_EarlyInteraction,
    count(distinct F_EarlyTicketFlag) as Unique_EarlyTicket,--
    count(distinct F_BillClaim) as Unique_BillClaim,--
    count(distinct noplanchange) as NoPlan,--
    count(distinct F_MRCChange) as Unique_MRCChange,--
    count (distinct F_MountingBillFlag) as Unique_MountingBill--
    ,B_FMCTYPE, E_FMCTYPE, First_Sales_Chnl_EOM, First_sales_CHNL_BOM, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , SALES_CHANNEL,sales_channel_so
from FullTable_Adj
WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' and Fixedchurntype != 'Fixed Involuntary Churner') or  Fixedchurntype is null) and finalchurnflag !='Fixed Churner'
Group by 1,2,3,4,5,6,7,8,9,B_FMCTYPE, E_FMCTYPE, First_Sales_Chnl_EOM, First_sales_CHNL_BOM, Last_Sales_CHNL_EOM, Last_Sales_CHNL_BOM , SALES_CHANNEL,sales_channel_so
order by 1
