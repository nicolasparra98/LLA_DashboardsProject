--CREATE OR REPLACE TABLE
--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-04_Cabletica_Final_Sprint7_Table_CX_DashboardInput_v3` AS

WITH 

FMC_Table AS(
  SELECT *,
  "CT" AS opco,"Costa_Rica" AS market,"Large" AS marketSize,"Fixed" AS product,"B2C" AS biz_unit,
  Case when MainMovement="New Customer" THEN Fixed_Account Else null end as Gross_Adds,
  Case when Fixed_account is not null then Fixed_Account Else null end as Active_Base
  ,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech
  FROM(select *,concat(coalesce(b_finaltechflag,''),coalesce(e_finaltechflag,'')) as tech_concat
   FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-08_Cabletica_Final_Table_DashboardInput_v3`)
   --`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`)
)
,Sprint3_KPIs as(
  select distinct Month,e_finaltechflag as tech,sum(activebase) as activebase,sum(sales) as unique_sales,sum(MountingBills) as unique_mountingbills,
  sum(Long_Installs) as unique_longinstalls,sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,
  sum(BillClaim) as unique_billclaim,sum(MRC_Change) as unique_mrcchange,sum(NoPlan_Changes) as noplan
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
  Where Month<>"2020-12-01" and Month<>"2022-06-01" and e_finaltechflag is not null
  group by 1,2
)
,S3_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(activebase) as activebase,sum(unique_mrcchange) as mrc_change,sum(noplan) as noplan_customers,sum(unique_sales) as unique_sales,sum(unique_longinstalls) as unique_longinstalls,sum(unique_mountingbills) as unique_mountingbills,sum(unique_earlyticket) as unique_earlyticket,sum(unique_earlyinteraction) as unique_earlyinteraction,
  round(safe_divide(sum(unique_mrcchange),sum(noplan)),4) as Customers_w_MRC_Changes,round(safe_divide(sum(unique_mountingbills),sum(activebase)),4) as MountingBills,round(safe_divide(sum(unique_longinstalls),sum(unique_sales)),4) as breech_cases_installs,round(safe_divide(sum(unique_earlyticket),sum(unique_sales)),4) as Early_Tech_Tix,round(safe_divide(sum(unique_earlyinteraction),sum(unique_sales)),4) as New_Customer_Callers
  From Sprint3_KPIs group by 1,2,3,4,5,6
)
,S3_CX_KPIs_Network as(
  select distinct Month,Tech,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  activebase,unique_mrcchange as mrc_change,noplan as noplan_customers,unique_sales,unique_longinstalls,unique_mountingbills,
  unique_earlyticket,unique_earlyinteraction,
  round(safe_divide(unique_mrcchange,noplan),4) as Customers_w_MRC_Changes,round(safe_divide(unique_mountingbills,activebase),0) as MountingBills,
  round(safe_divide(unique_longinstalls,unique_sales),4) as breech_cases_installs,round(safe_divide(unique_earlyticket,unique_sales),4) as Early_Tech_Tix,
  round(safe_divide(unique_earlyinteraction,unique_sales),4) as New_Customer_Callers
  From Sprint3_KPIs 
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
,Sprint3_Sales_KPIs as(
  select distinct Sales_Month as Month,e_finaltechflag as tech,sum(sales) as unique_sales,sum(Long_Installs) as unique_longinstalls,
  sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,sum(Soft_Dx) as unique_softdx
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
  Where sales_Month>="2021-01-01" and e_finaltechflag is not null
  group by 1,2
)
,S3_Sales_CX_KPIs as(
  select distinct safe_cast(Month as string) as Month,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(unique_sales) as unique_sales,sum(unique_longinstalls) as unique_longinstalls,sum(unique_earlyticket) as unique_earlyticket,sum(unique_earlyinteraction) as unique_earlyinteraction,sum(unique_softdx) as unique_softdx,
  round(safe_divide(sum(unique_longinstalls),sum(unique_sales)),4) as breech_cases_installs,round(safe_divide(sum(unique_earlyticket),sum(unique_sales)),4) as Early_Tech_Tix,
  round(safe_divide(sum(unique_earlyinteraction),sum(unique_sales)),4) as New_Customer_Callers,round(safe_divide(sum(unique_softdx),sum(unique_sales)),4) as New_Sales_to_Soft_Dx
  From Sprint3_Sales_KPIs group by 1,2,3,4,5,6
)
,S3_Sales_CX_KPIs_Network as(
  select distinct safe_cast(Month as string) as Month,Tech,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  unique_sales,unique_longinstalls,unique_earlyticket,unique_earlyinteraction,unique_softdx,
  round(safe_divide(unique_longinstalls,unique_sales),4) as breech_cases_installs,round(safe_divide(unique_earlyticket,unique_sales),4) as Early_Tech_Tix,
  round(safe_divide(unique_earlyinteraction,unique_sales),4) as New_Customer_Callers,round(safe_divide(unique_softdx,unique_sales),4) as New_Sales_to_Soft_Dx
  From Sprint3_Sales_KPIs 
)
,Sprint5_KPIs as(
  select Month,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech,sum(activebase) as activebase, sum(TwoCalls_Flag)+sum(MultipleCalls_Flag) as RepeatedCallers,
  sum(TicketDensity_Flag) as numbertickets
  from(select *,concat(coalesce(b_finaltechflag,''),coalesce(e_finaltechflag,'')) as tech_concat
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint5_Table_DashboardInput_v2`)
  group by 1,2
)
,S5_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(activebase) as fixed_acc,sum(repeatedcallers) as repeat_callers,sum(numbertickets) as tickets,
  safe_divide(sum(RepeatedCallers),sum(activebase)) as Repeated_Callers,safe_divide(sum(numbertickets),sum(activebase))*100 as Tech_Tix_per_100_Acct
  From Sprint5_KPIs where tech is not null
  group by 1
)
,S5_CX_KPIs_Network as(
  select distinct Month,Tech,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(activebase) as fixed_acc,sum(repeatedcallers) as repeat_callers,sum(numbertickets) as tickets,
  sum(safe_divide(RepeatedCallers,activebase)) as Repeated_Callers,safe_divide(sum(numbertickets),sum(activebase))*100 as Tech_Tix_per_100_Acct
  From Sprint5_KPIs where tech is not null
  group by 1,2
)
,Additional_KPIs as(
  Select Distinct Month,sum(FixedRGUs) as FixedRGUs,sum(TechCalls) as TechCalls,sum(CareCalls) as CareCalls,sum(BillVariations) as BillVariations
  ,sum(BillingCalls) as BillingCalls,sum(AllBillingCalls) as AllBillingCalls,sum(FTR_Billing) as FTR_Billing
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Additional_Cx_Table_DashboardInput_v2`
  group by 1
)
,Additional_CX_KPIs as(
  Select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"Large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(FixedRGUs) as unique_FixedRGUs,sum(TechCalls) as unique_TechCalls,sum(CareCalls) as unique_CareCalls,
  sum(BillVariations) as unique_BillVariations,sum(BillingCalls) as unique_BillingCallsBillVariations,
  sum(AllBillingCalls) as unique_allbillingcalls,sum(FTR_Billing) as unique_FTR_Billing
  From Additional_KPIs
  group by 1,2,3,4,5
)

############################################################################### New KPIs ##################################################################################

,FIXED_DATA AS(
SELECT  cast(date(cast(month as string)) as string) as month,
        market,
        network,
        Total_Subscribers as total_subscribers,
        safe_cast(Assisted_Installations as int64) as assisted_instalations,
        safe_cast(MTTI as int64) as mtti,
        safe_cast(Truck_Rolls as int64) as truck_rolls,
        safe_cast(MTTI as int64) as mttr,
        safe_cast(SCR as float64) as scr,
        safe_cast(i_ELF_28days_ as float64) as  i_elf_28days,
        safe_cast(r_ELF_28days_ as float64) as r_elf_28days,
        safe_cast(i_Sl as float64) as i_sl,
        safe_cast(r_Sl as float64) as r_sl
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220930_Service_Delivery_Monthly_KPIs`
)

,service_delivery as(
SELECT  distinct month as Month,
        Network,
        "CT" as Opco,
        "Costa_Rica" as Market,
        "Large" as MarketSize,
        "Fixed" as Product,
        "B2C" as Biz_Unit,
        --total_subscribers as Total_Users,
        assisted_instalations as Install,
        mtti as MTTI,
        --assisted_instalations*mtti as Inst_MTTI,
        truck_rolls as Repairs,
        mttr as MTTR,
        --truck_rolls*mttr as Rep_MTTR,
        scr as Repairs_1k_rgu,
        (100-i_elf_28days)/100 as FTR_Install,
        (100-r_elf_28days)/100 as FTR_Repair,
        (i_sl/assisted_instalations) as Installs_SL,
        (r_sl/truck_rolls) as Repairs_SL,
        --(100-i_elf_28days)*assisted_instalations as FTR_Install_M,
        --(100-r_elf_28days)*truck_rolls as FTR_Repair_M,
        i_sl as Inst_SL,
        r_sl as Rep_SL
FROM    FIXED_DATA
WHERE   market = "Costa Rica" 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1,2,3
)

,wanda_kpis as(
 select distinct cast(date(parse_date('%Y%m%d',cast(month as string))) as string) as month,'CT' as Opco,'Costa_Rica' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network
 from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-09_Cabletica_WandaDashboard` where opco='LCR'
)
,nps_kpis as(
select distinct cast(date(parse_date('%Y%m%d',cast(month as string))) as string) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-05_Cabletica_Sprint7_Table_NPS` where opco='CT')
########################################################################### All Flags KPIs ################################################################################
--Prev Calculated
,GrossAdds_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,
  count(distinct Gross_Adds) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,GrossAdds_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,
  count(distinct Gross_Adds) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Tech as network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Flag1 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Flag2 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Network1 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Tech as Network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Network2 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Tech as Network from FMC_Table where tech is not null group by 1,2,3,4,5,6,7,8,9,15
)
,TechTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,
  round(Tech_Tix_per_100_Acct,4) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network from S5_CX_KPIs
)
,TechTickets_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,
  round(Tech_Tix_per_100_Acct,4) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network
)
,MRCChanges_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,
round(Customers_w_MRC_Changes,4) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network From S3_CX_KPIs
)
,MRCChanges_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,
round(Customers_w_MRC_Changes,4) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Tech as Network From S3_CX_KPIs_Network
)
,SalesSoftDx_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,
  round(New_Sales_to_Soft_Dx,4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,'OVERALL' as Network From S3_Sales_CX_KPIs
)
,SalesSoftDx_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,
  round(New_Sales_to_Soft_Dx,4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,Tech as Network From S3_Sales_CX_KPIs_Network
)
,EarlyIssues_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,
  round(New_Customer_Callers,4) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,'OVERALL' as Network From S3_Sales_CX_KPIs
)
,EarlyIssues_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,
  round(New_Customer_Callers,4) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,Tech as Network From S3_Sales_CX_KPIs_Network
)
,LongInstall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,
  round(breech_cases_installs,4) as kpi_meas,unique_longinstalls as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,'OVERALL' as Network From S3_Sales_CX_KPIs
)
,LongInstall_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,
  round(breech_cases_installs,4) as kpi_meas,unique_longinstalls as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-1' as Kpi_delay_display,tech as Network From S3_Sales_CX_KPIs_Network
)
,EarlyTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,
  round(early_tech_tix,4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,'OVERALL' as Network From S3_Sales_CX_KPIs
)
,EarlyTickets_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,
  round(early_tech_tix,4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,tech as Network From S3_Sales_CX_KPIs_Network
)
,RepeatedCall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,
  round(Repeated_Callers,4) as kpi_meas,repeat_callers as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,'OVERALL' as Network From S5_CX_KPIs
)
,RepeatedCall_Network as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,
  round(Repeated_Callers,4) as kpi_meas,repeat_callers as kpi_num,fixed_acc as kpi_den,null as KPI_Sla,'M-2' as Kpi_delay_display,tech as Network From S5_CX_KPIs_Network
)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den,'M-0' as Kpi_delay_display,'OVERALL' as Network,null as kpi_sla  from fmc_table)

,BillingCallsPerBillVariation_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,
  round(sum(unique_BillingCallsBillVariations)/sum(unique_BillVariations),4) as kpi_meas,unique_BillingCallsBillVariations as kpi_num,unique_BillVariations as kpi_den,
  null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network
  From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,FTRBilling_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name,
  sum(unique_FTR_Billing)/sum(unique_allbillingcalls) as kpi_meas,unique_FTR_Billing as kpi_num,unique_allbillingcalls as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network 
  From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,MountingBill_Flag as(
select distinct  month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,MountingBills as kpi_meas,unique_mountingbills as kpi_num,activebase as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network from S3_CX_KPIs
)
,MountingBill_Network as(
select distinct  month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,MountingBills as kpi_meas,unique_mountingbills as kpi_num,activebase as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,tech as Network from S3_CX_KPIs_Network)

,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI_sl as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI_SL' as kpi_name, installs_sl as kpi_meas, inst_sl as kpi_num,install as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,mttr_sl as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR_SL' as kpi_name, repairs_sl as kpi_meas, rep_sl as kpi_num,repairs as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,Network from service_delivery)


############################################################## Join Flags ###########################################################################

,Join_DNA_KPIS as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From GrossAdds_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From ActiveBase_Flag1
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From ActiveBase_Flag2)
)

,Join_Sprints_KPIs as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network From Join_DNA_kpis
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from TechTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MRCChanges_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from SalesSoftDx_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from EarlyIssues_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from LongInstall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from EarlyTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from RepeatedCall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MountingBill_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from BillingCallsPerBillVariation_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from FTRBilling_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from OutlierRepair_Flag
  )
)
,Join_New_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from join_sprints_kpis
--union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from payments
)
)
---NotCalculated kpis

--BUY

,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='tBuy')

,mttb as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,Buyingcalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, null as kpi_meas, null as kpi_num,null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--GET

,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='tInstall')

,selfinstalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,installscalls as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

--PAY

,MTTBTR as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='tpay')

--Support-call
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='tHelp_Care')


--support-Tech

,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='tHelp_repair')

--use
,highrisk as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)

,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='pNPS')

--Wanda's Dashboard

,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from wanda_kpis where kpi_name='CC_SL_Care')

,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from wanda_kpis where kpi_name='CC_SL_Tech')

,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo  from wanda_kpis where kpi_name='Chatbot_Containment_Care')

,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Care' as kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo  from wanda_kpis where kpi_name='FRC_Care')

,chahtbottech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo  from wanda_kpis where kpi_name='Chatbot_Containment_Tech')

,TechCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo  from wanda_kpis where kpi_name='Tech_Calls_Intensity')

,CareCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla,Kpi_delay_display,Network,extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo  from wanda_kpis where kpi_name='Care_Calls_Intensity')

,paymentsnull as(
select distinct month,'CT' as Opco, 'Costa_Rica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'pay' as journey_waypoint,'Digital_Payments' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den,null as KPI_Sla,'M-0' as Kpi_delay_display,'OVERALL' as Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from fmc_table)
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,null as kpi_den,null as kpi_sla,Kpi_delay_display,Network,	extract (year from date(Month)) as ref_year, extract(month from date(month)) as ref_mo from nps_kpis where kpi_name='rNPS')

,All_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from Join_Sprints_KPIs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tBuy
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from mttb
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from Buyingcalls
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTI
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTR
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTI_sl
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTR_sl
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tinstall
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from ftr_installs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from installs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from selfinstalls
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from installscalls
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from MTTBTR
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from tpay
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from helpcare
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from frccare
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from helprepair
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from ftrrepair
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from justrepairs
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from repairs1k
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from highrisk
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from pnps
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from cccare
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from cctech
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from chatbot
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from chahtbottech
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from paymentsnull
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from rnps
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from TechCall1kRGU_Flag
union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from CareCall1kRGU_Flag
))

--,Join_Technology as(
select * from(select * from All_KPIs union all select * from GrossAdds_Network union all select * from ActiveBase_Network1 union all select * from ActiveBase_Network2 union all select * from TechTickets_Network union all select * from MRCChanges_Network union all select * from SalesSoftDx_Network union all select * from EarlyIssues_Network union all select * from LongInstall_Network union all select * from EarlyTickets_Network union all select * from RepeatedCall_Network union all select * from MountingBill_Network )
