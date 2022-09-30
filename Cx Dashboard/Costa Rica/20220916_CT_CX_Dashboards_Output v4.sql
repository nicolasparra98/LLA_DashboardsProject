with
cx as(
  select *
  from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-04_Cabletica_Final_Sprint7_Table_CX_DashboardInput_v3`
)
,digital_sales as(
select cast(date(parse_date('%Y%m%d',cast(month as string))) as string) as month,opco,market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,kpi_name,kpi_meas,null as kpi_num,null as kpi_den,kpi_delay_display,kpi_sla,network
from `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-08-10_Cabletica_DigitalPaymentsDashboard` where opco='CT'
)
,ecommerce as(
select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'digital_shift' as facet,'buy' as journey_waypoint,'e-Commerce' as kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla,Kpi_delay_display,Network from digital_sales
)
,all_kpis as(
select Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,network from cx union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,network from ecommerce
)
,Gross_adds_disc as(
select distinct month,network,'Gross_Adds' as kpi_name,kpi_meas as div from cx where facet='contact_drivers' and kpi_name='Active_Base')
,disclaimer_fields as(
select *,concat(cast(round(kpi_disclaimer_meas*100,2) as string),'% of base') as kpi_disclaimer_display
from(select j.*,case when j.kpi_name='Gross_Adds' then round(safe_divide(j.kpi_meas,g.div),4) else null end as kpi_disclaimer_meas
from all_kpis j left join Gross_adds_disc g on j.month=g.month and j.network=g.network and j.kpi_name=g.kpi_name)
)

select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,journey_waypoint,facet,kpi_name,kpi_meas,kpi_num,kpi_den,KPI_Sla, kpi_delay_display,kpi_disclaimer_display,kpi_disclaimer_meas,Network,extract(year from date(Month)) as ref_year,extract(month from date(month)) as ref_mo,null as kpi_sla_below_threshold,null as kpi_sla_middling_threshold,null as kpi_sla_above_threshold,null as kpi_sla_far_below_threshold,null as kpi_sla_far_above_threshold
from disclaimer_fields
where date(month)>=date('2022-01-01') --and kpi_name ='FTR_Repair' and Network = 'OVERALL'
order by 1,kpi_name
