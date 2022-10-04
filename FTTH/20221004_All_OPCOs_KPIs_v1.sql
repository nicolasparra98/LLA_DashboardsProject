WITH
-------------------------------------------------------- BASES NECESARIAS -------------------------------------------------------------------

FMC_Table_CWP AS ( 
SELECT *,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,
case when fixedmainmovement='4.New Customer' then fixedaccount else null end as Gross_Adds,
case when fixedaccount is not null then fixedaccount else null end as Active_Base,
case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,
case when fixedchurntype = '1. Fixed Voluntary Churner' then 1 else null end as Vol_Churners,
case when fixedchurntype = '2. Fixed Involuntary Churner' then 1 else null end as Invol_Churners
from( select *,concat(coalesce(b_final_techflag,''),coalesce(e_final_techflag,'')) as tech_concat
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" where month=date(dt))
)

,FMC_Table_CWC AS (
SELECT *,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,
case when mainmovement='4.New Customer' then fixed_account else null end as Gross_Adds,
case when fixed_account is not null then fixed_account else null end as Active_Base,
case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,
case when fixedchurntypeflag = '1.Fixed Voluntary Churner' then 1 else null end as Vol_Churners,
case when fixedchurntypeflag = '2. Fixed Involuntary Churner' then 1 else null end as Invol_Churners
from(select *,concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) as tech_concat
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" where month = date(dt))
)

,FIXED_DATA AS(
SELECT  date_trunc('month',cast(date_parse(CASE WHEN month  = 'nan' then null else month end,'%m/%d/%y') as date)) as month,
        market,
        network,
        cast(CASE WHEN "total subscribers"  = 'nan' then null else "total subscribers" end as double) as total_subscribers,
        cast(CASE WHEN "assisted installations"  = 'nan' then null else "assisted installations" end as double) as assisted_instalations,
        cast(CASE WHEN mtti  = 'nan' then null else mtti end as double) as mtti,
        cast(CASE WHEN "truck rolls"  = 'nan' then null else "truck rolls" end as double) as truck_rolls,
        cast(CASE WHEN mttr  = 'nan' then null else mttr end as double) as mttr,
        cast(CASE WHEN scr  = 'nan' then null else scr end as double) as scr,
        cast(CASE WHEN "i-elf(28days)"  = 'nan' then null else "i-elf(28days)" end as double) as i_elf_28days,
        cast(CASE WHEN "r-elf(28days)"  = 'nan' then null else "r-elf(28days)" end as double) as r_elf_28days,
        cast(CASE WHEN "i-sl"  = 'nan' then null else "i-sl" end as double) as i_sl,
        cast(CASE WHEN "r-sl"  = 'nan' then null else "r-sl" end as double) as r_sl
FROM "lla_cco_int_san"."cwp_ext_servicedelivery_monthly"
)

-------------------------------------------------------- TRANSFORMACIONES -------------------------------------------------------------------

----- Panama
,FMC_CWP_Network  AS (
SELECT month, Opco, Tech as Network, count(distinct Active_Base) as Active_Base, sum(Vol_Churners) as Voluntary_Churners, sum(Invol_Churners) as Involuntary_Churners
FROM FMC_Table_CWP 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

,FMC_CWP_Overall  AS (
SELECT month, Opco, 'OVERALL' as Network, count(distinct Active_Base) as Active_Base, sum(Vol_Churners) as Voluntary_Churners, sum(Invol_Churners) as Involuntary_Churners
FROM FMC_Table_CWP 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

----- Jamaica
,FMC_CWC_Network  AS (
SELECT month, Opco, Tech as Network, count(distinct Active_Base) as Active_Base, sum(Vol_Churners) as Voluntary_Churners, sum(Invol_Churners) as Involuntary_Churners
FROM FMC_Table_CWC 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

,FMC_CWC_Overall  AS (
SELECT month, Opco, 'OVERALL' as Network, count(distinct Active_Base) as Active_Base, sum(Vol_Churners) as Voluntary_Churners, sum(Invol_Churners) as Involuntary_Churners
FROM FMC_Table_CWC 
WHERE Tech IN ('COPPER','FTTH','HFC')
GROUP BY 2,1,3
ORDER BY 2,1,3
)

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
FROM    FIXED_DATA
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
SELECT  A.*, B.Active_Base as Customers, B.Voluntary_Churners, B.Involuntary_Churners
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

SELECT * FROM Result_Table WHERE year(month) >= 2022 
