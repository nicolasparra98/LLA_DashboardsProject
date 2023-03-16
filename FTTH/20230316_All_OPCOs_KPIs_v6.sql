WITH
-------------------------------------------------------- BASES NECESARIAS -------------------------------------------------------------------

FMC_Table_CWP AS ( 
SELECT *,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,
case when fixedmainmovement='4.New Customer' then fixedaccount else null end as Gross_Adds,
case when fixedaccount is not null and b_fixedtenure = 'Early-Tenure' then fixedaccount else null end as Active_Base_Early,
case when fixedaccount is not null and b_fixedtenure = 'Mid-Tenure' then fixedaccount else null end as Active_Base_Mid,
case when fixedaccount is not null and b_fixedtenure = 'Late-Tenure' then fixedaccount else null end as Active_Base_Late,
case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,
case when fixedchurntype = '1. Fixed Voluntary Churner' then 1 else null end as Vol_Churners,
case when fixedchurntype = '1. Fixed Voluntary Churner' and b_fixedtenure = 'Early-Tenure' then 1 else null end as Early_Vol_Churners,
case when fixedchurntype = '1. Fixed Voluntary Churner' and b_fixedtenure = 'Mid-Tenure' then 1 else null end as Mid_Vol_Churners,
case when fixedchurntype = '1. Fixed Voluntary Churner' and b_fixedtenure = 'Late-Tenure' then 1 else null end as Late_Vol_Churners,
case when fixedchurntype = '2. Fixed Involuntary Churner' then 1 else null end as Invol_Churners,
case when fixedchurntype = '2. Fixed Involuntary Churner' and b_fixedtenure = 'Early-Tenure' then 1 else null end as Early_Invol_Churners,
case when fixedchurntype = '2. Fixed Involuntary Churner' and b_fixedtenure = 'Mid-Tenure' then 1 else null end as Mid_Invol_Churners,
case when fixedchurntype = '2. Fixed Involuntary Churner' and b_fixedtenure = 'Late-Tenure' then 1 else null end as Late_Invol_Churners
from( select *,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev" where month=date(dt))
)

,FMC_Table_CWC AS (
SELECT *,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,
case when mainmovement='4.New Customer' then fixed_account else null end as Gross_Adds,
case when fixed_account is not null and b_fixedtenuresegment = 'Early-Tenure' then fixed_account else null end as Active_Base_Early,
case when fixed_account is not null and b_fixedtenuresegment = 'Mid-Tenure' then fixed_account else null end as Active_Base_Mid,
case when fixed_account is not null and b_fixedtenuresegment = 'Late-Tenure' then fixed_account else null end as Active_Base_Late,
case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,
case when fixedchurntypeflag = '1.Fixed Voluntary Churner' then 1 else null end as Vol_Churners,
case when fixedchurntypeflag = '1.Fixed Voluntary Churner' and b_fixedtenuresegment = 'Early-Tenure' then 1 else null end as Early_Vol_Churners,
case when fixedchurntypeflag = '1.Fixed Voluntary Churner' and b_fixedtenuresegment = 'Mid-Tenure' then 1 else null end as Mid_Vol_Churners,
case when fixedchurntypeflag = '1.Fixed Voluntary Churner' and b_fixedtenuresegment = 'Late-Tenure' then 1 else null end as Late_Vol_Churners,
case when fixedchurntypeflag = '2. Fixed Involuntary Churner' then 1 else null end as Invol_Churners,
case when fixedchurntypeflag = '2. Fixed Involuntary Churner' and b_fixedtenuresegment = 'Early-Tenure' then 1 else null end as Early_Invol_Churners,
case when fixedchurntypeflag = '2. Fixed Involuntary Churner' and b_fixedtenuresegment = 'Mid-Tenure' then 1 else null end as Mid_Invol_Churners,
case when fixedchurntypeflag = '2. Fixed Involuntary Churner' and b_fixedtenuresegment = 'Late-Tenure' then 1 else null end as Late_Invol_Churners
from(select *,concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) as tech_concat
FROM "lla_cco_int_ana_dev"."cwc_fmc_churn_dev" where month = date(dt))
)

,service_delivery AS(
SELECT  DATE_TRUNC('MONTH', cast(DATE_PARSE(CAST(month AS VARCHAR(10)), '%m/%d/%Y') as date)) as month,
        market,
        network,
        cast("total subscribers" as double) as total_subscribers,
        cast("assisted installations" as double) as assisted_instalations,
        cast(mtti as double) as mtti,
        cast("truck rolls" as double) as truck_rolls,
        cast(mttr as double) as mttr,
        cast(scr as double) as scr,
        cast("i-elf(28days)" as double) as i_elf_28days,
        cast("r-elf(28days)" as double) as r_elf_28days,
        cast("i-sl" as double) as i_sl,
        cast("r-sl" as double) as r_sl
FROM "lla_cco_int_san"."cwp_ext_servicedelivery_monthly_v2"
)

--------------- INTERACCIONES PANAMA ---------------------------------

,clean_interaction_time_cwp AS (
SELECT *,row_number() OVER (PARTITION BY account_id,cast(interaction_start_time as date) ORDER BY interaction_start_time desc) as row_num
FROM "db-stage-prod"."interactions_cwp"
WHERE (CAST(interaction_start_time AS VARCHAR) != ' ')
    AND interaction_start_time IS NOT NULL
    AND DATE(interaction_start_time) >= DATE('2022-01-01') 
    --AND DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) BETWEEN ((SELECT input_month FROM parameters)) AND ((SELECT input_month FROM parameters) + interval '1' month)
)

,interactions_inicial_cwp AS (
SELECT  *
        ,CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE) AS interaction_date, DATE_TRUNC('month',CAST(SUBSTR(CAST(interaction_start_time AS VARCHAR),1,10) AS DATE)) AS month
        ,CASE   WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%retenci%n%' THEN 'retention_claim'	 
                WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%restringido%' THEN 'service_restriction_claim'		
                WHEN interaction_purpose_descrip = 'CLAIM' AND interaction_disposition_info LIKE '%instalacion%' THEN 'installation_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%afiliacion%factura%' OR
                        interaction_disposition_info LIKE '%cliente%desc%' OR
                        interaction_disposition_info LIKE '%consulta%cuentas%' OR
                        interaction_disposition_info LIKE '%consulta%productos%' OR
                        interaction_disposition_info LIKE '%consumo%' OR
                        interaction_disposition_info LIKE '%info%cuenta%productos%' OR
                        interaction_disposition_info LIKE '%informacion%general%' OR
                        interaction_disposition_info LIKE '%pagar%on%line%' OR
                        interaction_disposition_info LIKE '%saldo%' OR
                        interaction_disposition_info LIKE '%actualizacion%datos%' OR
                        interaction_disposition_info LIKE '%traslado%linea%' OR
                        interaction_disposition_info LIKE '%transfe%cta%'
                        ) THEN 'account_info_or_balance_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%cargo%' OR
                        interaction_disposition_info LIKE '%credito%' OR
                        interaction_disposition_info LIKE '%facturaci%n%' OR
                        interaction_disposition_info LIKE '%pago%' OR
                        interaction_disposition_info LIKE '%prorrateo%' OR
                        interaction_disposition_info LIKE '%alto%consumo%' OR
                        interaction_disposition_info LIKE '%investigacion%interna%' OR
                        interaction_disposition_info LIKE '%cambio%descuento%'
                        ) THEN 'billing_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%venta%' OR
                        interaction_disposition_info LIKE '%publicidad%' OR
                        interaction_disposition_info LIKE '%queja%promo%precio%' OR
                        interaction_disposition_info LIKE '%promo%' OR
                        interaction_disposition_info LIKE '%promo%' OR
                        interaction_disposition_info LIKE '%cambio%de%precio%' OR  
                        interaction_disposition_info LIKE '%activacion%producto%' OR
                        interaction_disposition_info LIKE '%productos%servicios%-internet%' OR
                        interaction_disposition_info LIKE '%productos%servicios%suplementarios%' OR
                        interaction_disposition_info LIKE '%paytv%-tv%digital%hd%'
                        ) THEN 'sales_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '%queja%mala%atencion%' OR
                        interaction_disposition_info LIKE '%dunning%' OR
                        interaction_disposition_info LIKE '%consulta%reclamo%' OR
                        interaction_disposition_info LIKE '%apertura%reclamo%' OR
                        interaction_disposition_info LIKE '%horario%tienda%' OR
                        interaction_disposition_info LIKE '%consulta%descuento%' OR
                        interaction_disposition_info LIKE '%cuenta%apertura%reclamo%' OR
                        interaction_disposition_info LIKE '%actualizar%apc%' OR
                        interaction_disposition_info LIKE '%felicita%' 
                        ) THEN 'customer_service_claim'
                WHEN interaction_purpose_descrip = 'CLAIM' AND (
                        interaction_disposition_info LIKE '% da%no%' OR
                        interaction_disposition_info LIKE '%-da%no%' OR
                        interaction_disposition_info LIKE '%servicios%-da%os%' OR
                        interaction_disposition_info LIKE '%datos%internet%' OR
                        interaction_disposition_info LIKE '%equipo%recuperado%' OR
                        interaction_disposition_info LIKE '%escalami%niv%' OR
                        interaction_disposition_info LIKE '%queja%internet%' OR
                        interaction_disposition_info LIKE '%queja%linea%' OR
                        interaction_disposition_info LIKE '%queja%tv%' OR
                        interaction_disposition_info LIKE '%reclamos%tv%' OR
                        interaction_disposition_info LIKE '%serv%func%' OR
                        interaction_disposition_info LIKE '%soporte%internet%' OR
                        interaction_disposition_info LIKE '%soporte%linea%' OR
                        interaction_disposition_info LIKE '%tecn%' OR
                        interaction_disposition_info LIKE '%no%casa%internet%' OR
                        interaction_disposition_info LIKE '%no%energia%internet%' OR
                        interaction_disposition_info LIKE '%soporte%' OR
                        interaction_disposition_info LIKE '%intermiten%' OR
                        interaction_disposition_info LIKE '%masivo%'
                        ) THEN 'technical_claim'	
                WHEN interaction_purpose_descrip = 'CLAIM' THEN 'other_claims'							
                WHEN interaction_purpose_descrip = 'TICKET' THEN 'tech_ticket'							
                WHEN interaction_purpose_descrip = 'TRUCKROLL' THEN 'tech_truckroll'							
                END AS interact_category
FROM    (SELECT *,
        CASE    WHEN interaction_purpose_descrip = 'CLAIM' THEN REPLACE(CONCAT(LOWER(COALESCE(other_interaction_info4,' ')),'-',LOWER(COALESCE(other_interaction_info5,' '))),'  ','') 
                WHEN (interaction_purpose_descrip = 'TICKET' OR interaction_purpose_descrip = 'TRUCKROLL') THEN REPLACE(CONCAT(LOWER(COALESCE(other_interaction_info8,' ')),'-',LOWER(COALESCE(other_interaction_info9,' '))),'  ','') ELSE NULL END AS interaction_disposition_info
        FROM (SELECT * FROM clean_interaction_time_cwp HAVING row_num=1)
        WHERE interaction_id IS NOT NULL 
        )
)

,interactions_CWP AS (
SELECT  * 
        ,CASE   WHEN interact_category IN ('billing_claim','account_info_or_balance_claim','retention_claim') THEN 'Care Interaction'
                WHEN interact_category IN ('installation_claim','tech_ticket','tech_truckroll','technical_claim') THEN 'Technical Interaction'
                ELSE 'Others' END AS interaction_type
FROM interactions_inicial_cwp
)

,CWP_TECH_INTERACTIONS AS (
SELECT  DATE_TRUNC('MONTH',cast(interaction_start_time as date)) as month
        ,ACCOUNT_ID
        ,MAX(TECH_ROW_NUMB) AS TECH_INTERACTIONS
FROM    (SELECT *, row_number() OVER (PARTITION BY account_id, DATE_TRUNC('MONTH',cast(interaction_start_time as date)) ORDER BY interaction_start_time desc) AS TECH_ROW_NUMB
        FROM INTERACTIONS_CWP
        WHERE INTERACTION_TYPE = 'Technical Interaction'
        )
GROUP BY 1,2
ORDER BY 1,2
)

,CWP_CARE_INTERACTIONS AS (
SELECT  DATE_TRUNC('MONTH',cast(interaction_start_time as date)) as month
        ,ACCOUNT_ID
        ,MAX(care_ROW_NUMB) AS CARE_INTERACTIONS
FROM    (SELECT *, row_number() OVER (PARTITION BY account_id, DATE_TRUNC('MONTH',cast(interaction_start_time as date)) ORDER BY interaction_start_time desc) AS CARE_ROW_NUMB
        FROM INTERACTIONS_CWP
        WHERE INTERACTION_TYPE = 'Care Interaction'
        )
GROUP BY 1,2
ORDER BY 1,2
)

,CWP_TRUCKROLLS AS (
SELECT  DATE_TRUNC('MONTH',cast(interaction_start_time as date)) as month
        ,ACCOUNT_ID
        ,MAX(truckrolls_ROW_NUMB) AS truckrolls
FROM    (SELECT *, row_number() OVER (PARTITION BY account_id, DATE_TRUNC('MONTH',cast(interaction_start_time as date)) ORDER BY interaction_start_time desc) AS truckrolls_ROW_NUMB
        FROM INTERACTIONS_CWP
        WHERE interaction_purpose_descrip = 'TRUCKROLL'
        )
GROUP BY 1,2
ORDER BY 1,2
)

-------------------------------------------------------- TRANSFORMACIONES -------------------------------------------------------------------

----- Panama
,FMC_CWP_Network  AS (
SELECT month, Opco, Tech as Network, count(distinct Active_Base_Early) as Active_Base_Early, count(distinct Active_Base_Mid) as Active_Base_Mid, count(distinct Active_Base_Late) as Active_Base_Late, sum(Vol_Churners) as Total_Voluntary_Churners, sum(Early_Vol_Churners) as Early_Voluntary_Churners, sum(Mid_Vol_Churners) as Mid_Voluntary_Churners, sum(Late_Vol_Churners) as Late_Voluntary_Churners, sum(Invol_Churners) as Total_Involuntary_Churners, sum(Early_Invol_Churners) as Early_Involuntary_Churners, sum(Mid_Invol_Churners) as Mid_Involuntary_Churners, sum(Late_Invol_Churners) as Late_Involuntary_Churners, sum(early_tech_interactions) as early_tech_interactions, sum(mid_tech_interactions) as mid_tech_interactions, sum(late_tech_interactions) as late_tech_interactions, sum(early_care_interactions) as early_care_interactions, sum(mid_care_interactions) as mid_care_interactions, sum(late_care_interactions) as late_care_interactions, sum(early_truckrolls) as early_truckrolls, sum(mid_truckrolls) as mid_truckrolls, sum(late_truckrolls) as late_truckrolls
FROM    (SELECT E.*
                ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN F.truckrolls ELSE NULL END AS early_truckrolls
                ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN F.truckrolls ELSE NULL END AS mid_truckrolls
                ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN F.truckrolls ELSE NULL END AS late_truckrolls
        FROM    (SELECT C.*
                        ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN D.care_interactions ELSE NULL END AS early_care_interactions
                        ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN D.care_interactions ELSE NULL END AS mid_care_interactions
                        ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN D.care_interactions ELSE NULL END AS late_care_interactions
                FROM    (SELECT A.*
                            ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN B.tech_interactions ELSE NULL END AS early_tech_interactions
                            ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN B.tech_interactions ELSE NULL END AS mid_tech_interactions
                            ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN B.tech_interactions ELSE NULL END AS late_tech_interactions
                        FROM FMC_Table_CWP A LEFT JOIN CWP_TECH_INTERACTIONS B ON A.MONTH = B.MONTH AND A.fixedaccount = B.ACCOUNT_ID
                        ) C LEFT JOIN CWP_care_INTERACTIONS D ON C.MONTH = D.MONTH AND C.fixedaccount = D.ACCOUNT_ID
                ) E left join CWP_TRUCKROLLS F  ON E.MONTH = F.MONTH AND E.fixedaccount = F.ACCOUNT_ID
        )
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

,FMC_CWP_Overall  AS (
SELECT month, Opco, 'OVERALL' as Network, count(distinct Active_Base_Early) as Active_Base_Early, count(distinct Active_Base_Mid) as Active_Base_Mid, count(distinct Active_Base_Late) as Active_Base_Late, sum(Vol_Churners) as Total_Voluntary_Churners, sum(Early_Vol_Churners) as Early_Voluntary_Churners, sum(Mid_Vol_Churners) as Mid_Voluntary_Churners, sum(Late_Vol_Churners) as Late_Voluntary_Churners, sum(Invol_Churners) as Total_Involuntary_Churners, sum(Early_Invol_Churners) as Early_Involuntary_Churners, sum(Mid_Invol_Churners) as Mid_Involuntary_Churners, sum(Late_Invol_Churners) as Late_Involuntary_Churners, sum(early_tech_interactions) as early_tech_interactions, sum(mid_tech_interactions) as mid_tech_interactions, sum(late_tech_interactions) as late_tech_interactions, sum(early_care_interactions) as early_care_interactions, sum(mid_care_interactions) as mid_care_interactions, sum(late_care_interactions) as late_care_interactions, sum(early_truckrolls) as early_truckrolls, sum(mid_truckrolls) as mid_truckrolls, sum(late_truckrolls) as late_truckrolls
FROM    (select E.*
                ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN F.truckrolls ELSE NULL END AS early_truckrolls
                ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN F.truckrolls ELSE NULL END AS mid_truckrolls
                ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN F.truckrolls ELSE NULL END AS late_truckrolls
from (SELECT C.*
                ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN D.care_interactions ELSE NULL END AS early_care_interactions
                ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN D.care_interactions ELSE NULL END AS mid_care_interactions
                ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN D.care_interactions ELSE NULL END AS late_care_interactions
        FROM    (SELECT A.*
                        ,CASE WHEN b_fixedtenure = 'Early-Tenure' THEN B.tech_interactions ELSE NULL END AS early_tech_interactions
                        ,CASE WHEN b_fixedtenure = 'Mid-Tenure' THEN B.tech_interactions ELSE NULL END AS mid_tech_interactions
                        ,CASE WHEN b_fixedtenure = 'Late-Tenure' THEN B.tech_interactions ELSE NULL END AS late_tech_interactions
                FROM FMC_Table_CWP A LEFT JOIN CWP_TECH_INTERACTIONS B ON A.MONTH = B.MONTH AND A.fixedaccount = B.ACCOUNT_ID
                ) C LEFT JOIN CWP_care_INTERACTIONS D ON C.MONTH = D.MONTH AND C.fixedaccount = D.ACCOUNT_ID
        ) E left join CWP_TRUCKROLLS F  ON E.MONTH = F.MONTH AND E.fixedaccount = F.ACCOUNT_ID)
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

----- Jamaica
,FMC_CWC_Network  AS (
SELECT month, Opco, Tech as Network, count(distinct Active_Base_Early) as Active_Base_Early, count(distinct Active_Base_Mid) as Active_Base_Mid, count(distinct Active_Base_Late) as Active_Base_Late, sum(Vol_Churners) as Total_Voluntary_Churners, sum(Early_Vol_Churners) as Early_Voluntary_Churners, sum(Mid_Vol_Churners) as Mid_Voluntary_Churners, sum(Late_Vol_Churners) as Late_Voluntary_Churners, sum(Invol_Churners) as Total_Involuntary_Churners, sum(Early_Invol_Churners) as Early_Involuntary_Churners, sum(Mid_Invol_Churners) as Mid_Involuntary_Churners, sum(Late_Invol_Churners) as Late_Involuntary_Churners, null as early_tech_interactions, null as mid_tech_interactions, null as late_tech_interactions, null as early_care_interactions, null as mid_care_interactions, null as late_care_interactions, null as early_truckrolls, null as mid_truckrolls, null as late_truckrolls
FROM FMC_Table_CWC 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

,FMC_CWC_Overall  AS (
SELECT month, Opco, 'OVERALL' as Network, count(distinct Active_Base_Early) as Active_Base_Early, count(distinct Active_Base_Mid) as Active_Base_Mid, count(distinct Active_Base_Late) as Active_Base_Late, sum(Vol_Churners) as Total_Voluntary_Churners, sum(Early_Vol_Churners) as Early_Voluntary_Churners, sum(Mid_Vol_Churners) as Mid_Voluntary_Churners, sum(Late_Vol_Churners) as Late_Voluntary_Churners, sum(Invol_Churners) as Total_Involuntary_Churners, sum(Early_Invol_Churners) as Early_Involuntary_Churners, sum(Mid_Invol_Churners) as Mid_Involuntary_Churners, sum(Late_Invol_Churners) as Late_Involuntary_Churners, null as early_tech_interactions, null as mid_tech_interactions, null as late_tech_interactions, null as early_care_interactions, null as mid_care_interactions, null as late_care_interactions, null as early_truckrolls, null as mid_truckrolls, null as late_truckrolls
FROM FMC_Table_CWC 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

----- Costa Rica (se incluye cuando se apruebe la automatizacion)

----- Puerto Rico (Proximo desarrollo)

----- Union Table
,OPCOSs_Union_Table AS(
SELECT * FROM FMC_CWP_Network
UNION ALL 
SELECT * FROM FMC_CWP_Overall
UNION ALL
SELECT * FROM FMC_CWC_Network
UNION ALL 
SELECT * FROM FMC_CWC_Overall
)

----- CX Dashboard KPIs
,Service_Delivery_kpis as(
SELECT  distinct month as Month,
        CASE    WHEN market = 'Panama' then 'CWP'
                WHEN market = 'Jamaica' then 'CWC'
                WHEN market = 'Puerto Rico' then 'LCPR'
                WHEN market = 'Costa Rica' then 'CT' else 'Otro' end as OPCO,
        market,
        Network,
        round(truck_rolls,0) as Truck_Rolls,
        round(mtti,2) as MTTI,
        round((i_sl/assisted_instalations),4) as MTTI_SL,
        round(mttr,2) as MTTR,
        round((r_sl/truck_rolls),4) as MTTR_SL
        --total_subscribers as Total_Users,
        --round(assisted_instalations,0) as Install,
        --assisted_instalations*mtti as Inst_MTTI,
        --truck_rolls*mttr as Rep_MTTR,
        --round(scr,2) as Repairs_1k_rgu,
        --round((100-i_elf_28days)/100,4) as FTR_Install,
        --round((100-r_elf_28days)/100,4) as FTR_Repair,
        --(100-i_elf_28days)*assisted_instalations as FTR_Install_M,
        --(100-r_elf_28days)*truck_rolls as FTR_Repair_M,
        --round(i_sl,0) as Inst_SL,
        --round(r_sl,0) as Rep_SL
FROM    service_delivery
WHERE market in('Panama','Jamaica','Costa Rica','Puerto Rico')
        and Network in ('OVERALL','FTTH','HFC','COPPER')
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 1,3,2
)

,pNPS_kpi as(
select  distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,
        Opco,
        Network,
        kpi_name,
        kpi_meas as pNPS
from "lla_cco_int_san"."cwp_ext_nps_kpis"
where kpi_name in ('pNPS')
order by 1
)

,rNPS_kpi as(
select  distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,
        Opco,
        Network,
        kpi_name,
        kpi_meas as rNPS
from "lla_cco_int_san"."cwp_ext_nps_kpis"
where kpi_name in ('rNPS')
)

----- Results
,Result_Table as(
SELECT  A.*, B.Active_Base_Early as Early_Tenure_Customers, B.Active_Base_Mid as Mid_Tenure_Customers, B.Active_Base_Late as Late_Tenure_Customers, B.Total_Voluntary_Churners, B.Early_Voluntary_Churners, B.Mid_Voluntary_Churners, B.Late_Voluntary_Churners, B.Total_Involuntary_Churners, B.Early_Involuntary_Churners, B.Mid_Involuntary_Churners, B.Late_Involuntary_Churners, B.early_tech_interactions, B.mid_tech_interactions, B.late_tech_interactions, B.early_care_interactions, B.mid_care_interactions, B.late_care_interactions, B.early_truckrolls, B.mid_truckrolls, B.late_truckrolls
FROM    (SELECT  A.*, B.rNPS
        FROM    (SELECT A.*, B.pNPS
                        FROM Service_Delivery_kpis A LEFT JOIN pNPS_kpi B 
                        ON A.month = B.month 
                        AND A.OPCO = B.Opco
                        AND A.Network = B.Network ) A LEFT JOIN rNPS_kpi B 
                ON A.month = B.month 
                AND A.OPCO = B.Opco
                AND A.Network = B.Network) A LEFT JOIN OPCOSs_Union_Table B
        ON A.month = B.month 
        AND A.OPCO = B.Opco
        AND A.Network = B.Network
)

SELECT * FROM Result_Table 
WHERE month >= date('2022-01-01') 
ORDER BY 2,3,4,1
