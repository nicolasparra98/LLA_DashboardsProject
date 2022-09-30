with
--------------------------------Input Tables-------------------------------------------------------
fmc_table as(
select distinct month,'LCPR' as Opco,'Puerto_Rico' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,null as  facet,null as journey_waypoint,null as kpi_name,null as kpi_meas,null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display,null as Network
from "lla_cco_int_ana_prod"."cwp_fmc_churn_prod" 
)

,FIXED_DATA AS(
SELECT  date_trunc('month',cast(month as date)) as month,
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

,service_delivery as(
SELECT  distinct month as Month,
        Network,
        'LCPR' as Opco,
        'Puerto_Rico' as Market,
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
        round((100-i_elf_28days),4) as FTR_Install,
        round((100-r_elf_28days),4) as FTR_Repair,
        round((i_sl/assisted_instalations),4) as Installs_SL,
        round((r_sl/truck_rolls),4) as Repairs_SL,
        --(100-i_elf_28days)*assisted_instalations as FTR_Install_M,
        --(100-r_elf_28days)*truck_rolls as FTR_Repair_M,
        round(i_sl,0) as Inst_SL,
        round(r_sl,0) as Rep_SL
FROM    FIXED_DATA
WHERE   market = 'Puerto Rico' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1,2,3
)
,nps_kpis as(
select distinct date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,Network from "lla_cco_int_san"."cwp_ext_nps_kpis" where opco='LCPR')
,wanda_kpis as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,Opco,'Puerto_Rico' as market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,network from "lla_cco_int_san"."cwp_ext_nps_wanda"  where opco='LCPR')
,digital_sales as(
select date(date_parse(cast(month as varchar),'%Y%m%d')) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from "lla_cco_int_san"."cwp_ext_digitalsales" where opco='LCPR')
-------------------------------------Churn Dashboard kpis-----------------------------------------------------
,GrossAdds_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display, Network from fmc_table)
,ActiveBase_Flag1 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
,ActiveBase_Flag2 as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,TechTickets_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MRCChanges_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'Customers_w_MRC_Changes_5%+_Excl_Plan' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,SalesSoftDx_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Sales_to_Soft_Dx' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyIssues_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'New_Customer_Callers_2+calls_21Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,LongInstall_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Breech_Cases_Install_6+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,EarlyTickets_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'Early_Tech_Tix_-7Weeks' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,RepeatedCall_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'Repeat_Callers_2+Calls' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,OutlierRepair_Flag as (
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-tech' as journey_waypoint,'Breech_Cases_Repair_4+Days' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
,MountingBill_Flag as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'pay' as journey_waypoint,'Customers_w_Mounting_Bills' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network  from fmc_table)
-------------------------------------Service Delivery Kpis---------------------------------------------------
,installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'get' as journey_waypoint,'Installs' as kpi_name, Install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,MTTI as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI' as kpi_name, mtti as kpi_meas, null as kpi_num,null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,MTTI_SL as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'get' as journey_waypoint,'MTTI_SL' as kpi_name, installs_sl as kpi_meas, inst_sl as kpi_num,install as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftr_installs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'get' as journey_waypoint,'FTR_Installs' as kpi_name, ftr_install as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,justrepairs as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-tech' as journey_waypoint,'Repairs' as kpi_name, repairs as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_displa,Network from service_delivery)
,mttr as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR' as kpi_name, mttr as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,mttr_sl as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-tech' as journey_waypoint,'MTTR_SL' as kpi_name, repairs_sl as kpi_meas, rep_sl as kpi_num,repairs as kpi_den, 'M-0' as Kpi_delay_display, Network from service_delivery)
,ftrrepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-tech' as journey_waypoint,'FTR_Repair' as kpi_name, ftr_repair as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
,repairs1k as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-tech' as journey_waypoint,'Repairs_per_1k_RGU' as kpi_name, Repairs_1k_rgu as kpi_meas, null as kpi_num,	null as kpi_den, 'M-0' as Kpi_delay_display,Network from service_delivery)
-------------------------------------NPS Kpis-----------------------------------------------------
,tBuy as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tBuy')
,tinstall as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas,null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tInstall')
,tpay as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tPay' as kpi_name,kpi_meas as kpi_meas, null as kpi_num,	null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tpay')
,helpcare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_Care')
,helprepair as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,'tHelp_Repair' as kpi_name, kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='tHelp_repair')
,pnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas as kpi_meas, null as kpi_num,null as kpi_den, Kpi_delay_display,Network from nps_kpis where kpi_name='pNPS')
,rnps as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas, null as kpi_num,null as kpi_den,Kpi_delay_display,Network from nps_kpis where kpi_name='rNPS')
-------------------------------------Wanda Kpis-----------------------------------------------------
,cccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Care')
,cctech as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='CC_SL_Tech')
,chatbot as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Care')
,carecall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Care_Calls_Intensity')
,techcall as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Tech_Calls_Intensity')
,chahtbottech as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'support-tech' as journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='Chatbot_Containment_Tech')
,frccare as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'support-call' as journey_waypoint,'FCR_Care' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from wanda_kpis where kpi_name='FRC_Care')
-------------------------------------Other Kpis-----------------------------------------------------
,highrisk as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,payments as(
select distinct month,opco,market,marketsize,product,biz_unit,'digital_shift' as facet,'pay' as journey_waypoint,'Digital_Payments' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network,kpi_sla from digital_sales)
,ftr_billing as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'FTR_Billing' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,installscalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'get' as journey_waypoint,'Install_Calls/Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,MTTBTR as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'pay' as journey_waypoint,'MTTBTR' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,selfinstalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'get' as journey_waypoint,'Self_Installs' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,mttb as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'customer_time' as facet,'buy' as journey_waypoint,'MTTB' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,Buyingcalls as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'buy' as journey_waypoint,'Buying_Calls/GA' as kpi_name, kpi_meas, kpi_num,kpi_den, Kpi_delay_display, Network from fmc_table)
,billbill as(
select distinct  month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,kpi_meas,kpi_num,kpi_den,Kpi_delay_display,Network from fmc_table)
-----------------------------------------join--------------------------------------
,join_churn as (
select * from GrossAdds_Flag union all select * from ActiveBase_Flag1 union all select * from ActiveBase_Flag2 union all select * from TechTickets_Flag union all select * from MRCChanges_Flag union all select * from SalesSoftDx_Flag union all select * from EarlyIssues_Flag union all select * from LongInstall_Flag union all select * from EarlyTickets_Flag union all select * from RepeatedCall_Flag union all select * from OutlierRepair_Flag union all select * from MountingBill_Flag)
,join_service_delivery as(
select * from join_churn union all select * from installs union all select * from MTTI union all select * from ftr_installs union all select * from justrepairs union all select * from mttr union all select * from ftrrepair union all select * from repairs1k)
,join_nps as(
select * from join_service_delivery union all select * from tBuy union all select * from tinstall union all select * from tpay union all select * from helpcare union all select * from helprepair union all select * from pnps union all select * from rnps)
,join_wanda as(
select * from join_nps union all select * from billbill union all select * from cccare union all select * from cctech union all select * from chatbot union all select * from carecall union all select * from techcall union all select * from chahtbottech)
,join_others as(
select *,null as kpi_sla from(select * from join_wanda union all select * from highrisk union all select * from payments union all select * from frccare union all select * from ftr_billing union all select * from installscalls union all select * from MTTBTR union all select * from selfinstalls union all select * from  mttb union all select * from Buyingcalls union all select * from mtti_sl union all select * from mttr_sl)
union all select * from ecommerce
)
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den, kpi_sla,Kpi_delay_display,null as kpi_disclaimer_display,null as kpi_disclaimer_meas,Network,year(Month) as ref_year,month(month) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
--facet,journey_waypoint,kpi_name
from join_others
where month>=date('2022-01-01') --and (kpi_name like '%FTR%' or kpi_name like '%MTT%' or kpi_name in ('Installs', 'Repairs'))
order by 1,kpi_name
