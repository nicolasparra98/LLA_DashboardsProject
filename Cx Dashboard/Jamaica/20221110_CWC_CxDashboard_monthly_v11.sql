with -------------------------------------Previoulsy Calculated KPIs-------------------------------------------------
FMC_Table AS (
SELECT *,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla,null as Kpi_delay_display,case when waterfall_flag in ('Gross Ads') then fixed_account else null end as Gross_Adds,case when fixed_account is not null then fixed_account else null end as Active_Base,case when tech_concat LIKE '%FTTH%' then 'FTTH' when tech_concat NOT LIKE '%FTTH%' and tech_concat LIKE '%HFC%' then 'HFC' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat LIKE '%COPPER%' THEN 'COPPER' when tech_concat NOT LIKE '%FTTH%' and tech_concat NOT LIKE '%HFC%' AND tech_concat NOT LIKE '%COPPER%' AND tech_concat LIKE '%Wireless%' then 'Wireless' else null end as Tech
from(select *,concat(coalesce(b_final_tech_flag,''),coalesce(e_final_tech_flag,'')) as tech_concat
FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod"
where month = date(dt))
)

,Sprint3_Network as(
select distinct activebase_month as month, e_final_tech_flag as tech,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(activebase) as activebase,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,sum(Unique_NoPlanChanges) as noplan,sum(unique_mountingbills) as unique_mountingbill,sum(unique_mrcincrease) as unique_mrcchange
,round(sum(cast(unique_mrcincrease as double))/sum(cast(Unique_NoPlanChanges as double)),4) as Customers_w_MRC_Changes
,round(sum(cast(unique_mountingbills as double))/sum(cast(activebase as double)),4) as Customers_w_Mounting_Bills
,round(sum(cast(unique_softdx as double))/sum(cast(unique_sales as double)),4) as New_Sales_to_Soft_Dx
from "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix" where --activebase_month = date(dt) and 
e_final_tech_flag is not null group by 1,2,3,4,5,6,7
order by 1,2
)
,Sprint3_Network_install as(
select distinct install_month, e_final_tech_flag as tech,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,sum(activebase) as activebase,sum(unique_sales) as unique_sales,sum(unique_longinstall) as unique_longinstall,sum(unique_earlytickets) as unique_earlyticket
,round(sum(cast(unique_longinstall as double))/sum(cast(unique_sales as double)),4) as Breech_Cases_Install
,round(sum(cast(unique_earlytickets as double))/sum(cast(unique_sales as double)),4) as Early_Tech_Tix
from "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix" where --activebase_month = date(dt) and 
e_final_tech_flag is not null group by 1,2,3,4,5,6,7
order by 1,2
)
,Sprint3_KPIsM as (
select distinct activebase_month as month,sum(activebase) as activebase,sum(soft_dx) as soft_dx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_longinstall) as unique_longinstall,sum(unique_earlytickets) as unique_earlyticket,sum(Unique_NoPlanChanges) as noplan,sum(unique_mountingbills) as unique_mountingbill,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx,sum(unique_mrcincrease) as unique_mrcchange
from "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix"
where --activebase_month = date(dt) and 
e_final_tech_flag is not null group by 1
) 
,Sprint3_KPIsS as (
select distinct install_month as month,sum(unique_longinstall) as unique_longinstall,sum(unique_sales) as unique_sales, sum(unique_earlytickets) as unique_earlyticket
from "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix"
where --activebase_month = date(dt) and 
e_final_tech_flag is not null group by 1
)
,Sprint3_KPIsI as (
select distinct activebase_month as month,sum(soft_dx) as soft_dx,sum(unique_neverpaid) as unique_neverpaid,sum(unique_earlytickets) as unique_earlyticket,sum(unique_mountingbills) as unique_mountingbill,sum(unique_sales) as unique_sales,sum(unique_softdx) as unique_softdx
from "lla_cco_int_stg"."cwc_sprint3_03nov_FIXED_RepTix"
where --activebase_month = date(dt) and 
e_final_tech_flag is not null group by 1
)
,S3_CX_KPIsM as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_mrcchange,noplan,unique_mountingbill,activebase,round(cast(unique_mrcchange as double) / cast(noplan as double),4) as Customers_w_MRC_Changes,round(cast(unique_mountingbill as double) / cast(activebase as double),4) as Customers_w_Mounting_Bills
from Sprint3_KPIsM order by 1
)
,S3_CX_KPIsS as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_longinstall,unique_earlyticket,unique_sales,round(cast(unique_longinstall as double) / cast(unique_sales as double),4) as Breech_Cases_Install, round(cast(unique_earlyticket as double)/cast(unique_sales as double),4)
    as Early_Tech_Tix 
from Sprint3_KPIsS order by 1
)
,S3_CX_KPIsI as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,unique_softdx,unique_earlyticket,unique_sales,round(cast(unique_softdx as double) / cast(unique_sales as double),4) as New_Sales_to_Soft_Dx
from Sprint3_KPIsI order by 1
) 
,Sprint5_KPIs as(
select distinct Month,e_final_tech_flag as Tech,sum(activebase) fixed_accounts,sum(outlier_repairs) as outlier_repairs,sum(totaltickets) as numbertickets
from "lla_cco_int_stg"."cwc_sprint5_03nov_FIXED_RepTix"  where e_final_tech_flag is not null
group by 1,2 order by 1,2
)
,S5_CX_KPIs as(
select distinct month,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,sum(fixed_accounts) as fixed_accounts,sum(outlier_repairs) as outlier_repairs,sum(numbertickets) as numbertickets,round(cast(sum(outlier_repairs) as double) / cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair,round(cast(sum(numbertickets) as double) / cast(sum(fixed_accounts) as double),4)*100 as Tech_Tix_per_100_Acct
	from Sprint5_KPIs group by 1,2,3,4,5,6,7,8 order by 1,2,3,4,5,6
)
,S5_CX_KPIs_Network as(
select distinct month,Tech,'CWC' as Opco,'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,fixed_accounts,outlier_repairs,numbertickets,round(cast(sum(outlier_repairs) as double) / cast(sum(fixed_accounts) as double),4) as Breech_Cases_Repair,round(cast(sum(numbertickets) as double) / cast(sum(fixed_accounts) as double),4)*100 as Tech_Tix_per_100_Acct
	from Sprint5_KPIs group by 1,2,3,4,5,6,7,8,9,10,11,12 order by 1,2,3,4,5,6
)-----------------New KPIs
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'pay' as journey_waypoint,'digital_shift' as facet,'Digital_Payments' as kpi_name,null as KPI_Sla, 'M-0' as Kpi_delay_display,round(cast(count(distinct digital) as double) / cast(count(distinct pymt_cnt) as double) ,4) as kpi_meas,count( distinct digital) as kpi_num,count(distinct pymt_cnt) as kpi_den,'OVERALL' as Network
from(select date_trunc('month', date(dt)) as month,'CWC' as opco,'Jamaica' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as KPI_Sla, null as Kpi_delay_display,payment_doc_id as pymt_cnt,case when digital_nondigital = 'Digital' then payment_doc_id end as digital
FROM "db-stage-prod"."payments_cwc" where account_type = 'B2C' and country_name = 'Jamaica'
group by 1,2,3,4,5,6,7,digital_nondigital,payment_doc_id) group by 1,2,3,4,5,6,7,8,9,10,11,15
)
,nps_kpis as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from "lla_cco_int_san"."cwp_ext_nps_kpis" where opco='CWC') 
,wanda_kpis as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network,null as kpi_sla from "lla_cco_int_san"."cwp_ext_nps_wanda"  where opco='CWC')

,digital_sales as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from "lla_cco_int_san"."cwp_ext_digitalsales" where opco='CWC')
---------------------------------All Flags KPIs------------------------------------------------------------
-------Prev Calculated
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,0 as kpi_num,0 as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,count(distinct Gross_Adds) as kpi_meas,'OVERALL' as Network from fmc_table
Group by 1,2,3,4,5,6,7,8,9,10,15
)
,GrossAdds_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,count(distinct Gross_Adds) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network from fmc_table where tech is not null
Group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,0 as kpi_num,null as KPI_Sla, 'M-0' as Kpi_delay_display,0 as kpi_den, count(distinct Active_Base) as kpi_meas,'OVERALL' as Network from fmc_table where tech is not null
Group by 1,2,3,4,5,6,7,8,9,10,11,15
)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den,'OVERALL' as Network from fmc_table where tech is not null
Group by 1,2,3,4,5,6,7,8,9,10,11,15
)
,ActiveBase_Network1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name, count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network from fmc_table where tech is not null
Group by 1,2,3,4,5,6,7,8,9,15
)
,ActiveBase_Network2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network from fmc_table where tech is not null
Group by 1,2,3,4,5,6,7,8,9,15
)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct , 4) as kpi_meas,numbertickets as kpi_num,fixed_accounts as kpi_den,'OVERALL' as Network
from S5_CX_KPIs
)
,TechTickets_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,round(Tech_Tix_per_100_Acct , 4) as kpi_meas,numbertickets as kpi_num,fixed_accounts as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network 
from S5_CX_KPIs_Network
)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,round(Customers_w_MRC_Changes , 4) as kpi_meas,unique_mrcchange as kpi_num,noplan as kpi_den,'OVERALL' as Network	from S3_CX_KPIsm
)
,MRCChanges_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,round(Customers_w_MRC_Changes , 4) as kpi_meas,unique_mrcchange as kpi_num,noplan as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,tech as Network	from Sprint3_Network
)
,SalesSoftDx_Flag as(
select distinct date_add('month', 2, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-2' as Kpi_delay_display,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx , 4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den,'OVERALL' as Network	from S3_CX_KPIsi
)
,SalesSoftDx_Network as(
select distinct date_add('month', 2, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,round(New_Sales_to_Soft_Dx , 4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den,null as KPI_Sla, 'M-2' as Kpi_delay_display,tech as Network from Sprint3_Network
)
,LongInstall_Flag as(
select distinct date_add('month', 1, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-1' as Kpi_delay_display,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,round(breech_cases_install, 4) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den,'OVERALL' as Network	from S3_CX_KPIss
)
,LongInstall_Network as(
select distinct date_add('month', 1, install_month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,round(breech_cases_install , 4) as kpi_meas,unique_longinstall as kpi_num,unique_sales as kpi_den,null as KPI_Sla, 'M-1' as Kpi_delay_display,tech as Network	from Sprint3_Network_Install
)
,EarlyTickets_Flag as(
select distinct date_add('month', 1, month) as month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-1' as Kpi_delay_display,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,round(early_tech_tix , 4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den,'OVERALL' as Network from S3_CX_KPIsS
)
,EarlyTickets_Network as(
select distinct date_add('month', 1, install_month) as month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,round(early_tech_tix , 4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den,null as KPI_Sla, 'M-1' as Kpi_delay_display,tech as Network from Sprint3_Network_Install
)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,null as KPI_Sla, 'M-0' as Kpi_delay_display,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den,'OVERALL' as Network from S5_CX_KPIs
)
,RepeatedCall_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network
)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,round(Breech_Cases_Repair , 4) as kpi_meas,outlier_repairs as kpi_num,fixed_accounts as kpi_den,'OVERALL' as Network from S5_CX_KPIs
)
,OutlierRepair_Network as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,round(Breech_Cases_Repair , 4) as kpi_meas,outlier_repairs as kpi_num,fixed_accounts as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,Tech as Network from S5_CX_KPIs_Network
)
--Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,null as KPI_Sla, 'M-0' as Kpi_delay_display,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills , 4) as kpi_meas,unique_mountingbill as kpi_num,activebase as kpi_den,'OVERALL' as Network from S3_CX_KPIsm
)
,MountingBill_Network as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,round(Customers_w_Mounting_Bills , 4) as kpi_meas,unique_mountingbill as kpi_num,activebase as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display,tech as Network from Sprint3_Network
) ---------------------------------Join Flags-----------------------------------------------------------------
,Join_DNA_KPIs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den ,KPI_Sla,Kpi_delay_display,Network
from(select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from GrossAdds_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from ActiveBase_Flag1 union all	select month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from ActiveBase_Flag2)
)
,Join_Sprints_KPIs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network
from(select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from join_dna_kpis union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display,Network from TechTickets_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display,Network from MRCChanges_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from SalesSoftDx_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from LongInstall_Flag	union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network from EarlyTickets_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from RepeatedCall_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, Kpi_delay_display,Network from OutlierRepair_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,network from MountingBill_Flag union all select month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, 'M-0' as Kpi_delay_display,Network from payments)
)
,final_table as( 
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,  Kpi_delay_display,Network
from Join_Sprints_KPIs
where date_trunc('year', month) = date('2022-01-01') order by month
)

---NotCalculated kpis
--BUY
,more2calls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+Calls_21Days' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from digital_sales)
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,mttb as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
,Buyingcalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
--GET
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,selfinstalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
,installscalls as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
--PAY
,MTTBTR as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name, kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,ftr_billing as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
,billbill as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name, null as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as kpi_delay_display,'OVERALL' as Network from fmc_table)
--Support-call
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, kpi_meas as kpi_meas, null as kpi_num,null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
--support-Tech
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, kpi_meas as kpi_meas, null as kpi_num,null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
--use
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name, kpi_meas as kpi_meas, null as kpi_num,null as kpi_den,null as kpi_sla, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
--Wanda's Dashboard
,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Care')
,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Tech')
,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Care')
,carecall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='Care_Calls_Intensity')
,techcall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='Tech_Calls_Intensity')
,chahtbottech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Tech')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Care' as kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='FCR_Care')
,frctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Tech' as kpi_name,kpi_meas,kpi_num,kpi_den,kpi_sla,Kpi_delay_display,Network from wanda_kpis where kpi_name='FCR_Tech')
-------------------------Service Delivery--------------

,FIXED_DATA AS(
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
from "lla_cco_int_san"."cwp_ext_servicedelivery_monthly_v2" 
)

,service_delivery as(
SELECT  distinct month as Month,
        Network,
        'CWC' as Opco,
        'Jamaica' as Market,
        'Large' as MarketSize,
        'Fixed' as Product,
        'B2C' as Biz_Unit,
        --total_subscribers as Total_Users,
        round(assisted_instalations,0) as Install,
        round(mtti,2) as MTTI,
        --assisted_instalations*mtti as Inst_MTTI,
        round(truck_rolls,0) as Repairs,
        round(mttr,2) as MTTR,
        --truck_rolls*mttr as Rep_MTTR,
        round(scr,2) as Repairs_1k_rgu,
        round((100-i_elf_28days)/100,4) as FTR_Install,
        round((100-r_elf_28days)/100,4) as FTR_Repair,
        round((i_sl/assisted_instalations),4) as Installs_SL,
        round((r_sl/truck_rolls),4) as Repairs_SL,
        --(100-i_elf_28days)*assisted_instalations as FTR_Install_M,
        --(100-r_elf_28days)*truck_rolls as FTR_Repair_M,
        round(i_sl,0) as Inst_SL,
        round(r_sl,0) as Rep_SL
FROM    FIXED_DATA
WHERE   market = 'Jamaica' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1,2,3
)



--Service Delivery KPIs
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI_SL as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI_SL' as kpi_name, installs_sl as kpi_meas, inst_sl as kpi_num,install as kpi_den,null as KPI_Sla, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_Installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,Network from service_delivery)
,mttr_sl as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR_SL' as kpi_name, repairs_sl as kpi_meas, rep_sl as kpi_num,repairs as kpi_den, null as KPI_Sla,'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,Network from service_delivery)
--Nodes

,clean_interactions_base AS(
select *, row_number() OVER (PARTITION BY REGEXP_REPLACE(account_id,'[^0-9 ]',''), cast(interaction_start_time as date) ORDER BY interaction_start_time desc) as row_num
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%'
)

,nodes_initial_table as 
(SELECT date_trunc('Month', date(interaction_start_time)) as Month, interaction_id, account_id_2, concat(account_id_2, cast(date_trunc('Month', date(interaction_start_time))as varchar)) as Account_Month
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2 from (select * from clean_interactions_base having row_num = 1)
where lower(org_cntry) like '%jam%') where length (account_id_2) = 8
GROUP BY 1,account_id_2, interaction_id,4
)
,nodes_table as (
select date_trunc('Month', date(dt)) as Month, act_acct_cd, max(NR_LONG_NODE) as NR_LONG_NODE,max(case when account_id_2 is not null then 1 else 0 end) as customer_with_ticket,case when length(act_acct_cd) = 8 Then 'Cerilion' else 'Liberate' END AS CRM
from "db-analytics-prod"."tbl_fixed_cwc" t left join nodes_initial_table i on t.act_acct_cd = i.account_id_2 and date_trunc('Month', date(t.dt)) = i.Month
where org_cntry='Jamaica' AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and NR_LONG_NODE is not null AND NR_LONG_NODE <>'' AND NR_LONG_NODE <>' ' AND length(act_acct_cd)=8  
GROUP BY 1,act_acct_cd 
)
,grouped_by_node as (
select Month, CRM, count(distinct act_acct_cd) as customers_per_node, sum(customer_with_ticket) as customer_with_ticket, NR_LONG_NODE
from nodes_table
GROUP BY Month, NR_LONG_NODE, CRM
)
,final_nodes as (select Month, count(distinct NR_LONG_NODE) as nodes, sum(case when customer_with_ticket>0.06*customers_per_node then 1 else 0 end) overcharged_nodes
from grouped_by_node
where date_trunc('Year', date(Month)) = date('2022-01-01')
group by 1
order by 1 desc
)
,highrisk as(
select distinct month,'CWC' as Opco, 'Jamaica' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, round(cast(overcharged_nodes as double)/cast(nodes as double), 2) as kpi_meas, overcharged_nodes as kpi_num,	nodes as kpi_den, null as KPI_Sla, 'M-0' as Kpi_delay_display,'OVERALL' as Network from final_nodes group by 1,2,3,4,5,6,7,8,9,11,12,13,14,15
)
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,null as kpi_sla,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')

,final_full_kpis as (select * from final_table union all select * from more2calls union all select * from mttb union all select * from Buyingcalls union all select * from tbuy union all select * from ecommerce union all select * from MTTI union all select * from tinstall union all select * from ftr_installs union all select * from installs union all select * from selfinstalls union all select * from installscalls union all select * from MTTBTR union all select * from tpay union all select * from ftr_billing union all select * from helpcare union all select * from frccare union all select * from frctech union all select * from mttr  union all select * from helprepair union all select * from ftrrepair union all select * from repairs1k union all select * from highrisk union all select * from justrepairs union all select * from pnps union all select * from chahtbottech union all select * from techcall union all select * from carecall union all select * from chatbot union all select * from cctech union all select * from cccare union all select * from billbill union all select * from rnps  union all select * from mttr_sl union all select * from mtti_sl
 )
,Join_Technology as(
select * from final_full_kpis union all select * from GrossAdds_Network union all select * from activebase_network1 union all select * from activebase_network2 union all select * from TechTickets_Network union all select * from RepeatedCall_Network union all select * from OutlierRepair_Network union all select * from MRCChanges_Network union all select * from SalesSoftDx_Network union all select * from LongInstall_Network union all select * from EarlyTickets_Network union all select * from MountingBill_Network
)
,Gross_adds_disc as(
select distinct month,network,'Gross_Adds' as kpi_name,kpi_meas as div from join_technology where facet='contact_drivers' and kpi_name='Active_Base')

,disclaimer_fields as(
select *,concat(cast(round(kpi_disclaimer_meas*100,2) as varchar),'% of base') as kpi_disclaimer_display
from(select j.*,case when j.kpi_name='Gross_Adds' then round(j.kpi_meas/g.div,4) else null end as kpi_disclaimer_meas
from join_technology j left join Gross_adds_disc g on j.month=g.month and j.network=g.network and j.kpi_name=g.kpi_name)
)

select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den, KPI_Sla, kpi_delay_display,kpi_disclaimer_display,kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
from disclaimer_fields
--final_full_kpis
where month>=date('2022-01-01') --and kpi_name  in  ('FCR_Tech','FCR_Care')
order by 1,kpi_name
