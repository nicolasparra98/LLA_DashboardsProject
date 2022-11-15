select distinct month,
round((cast(sum(unique_softdx) as double)/cast(sum(unique_sales) as double)),4)*100 as Soft_Dx_KPI_2,
sum(unique_softdx),
sum(unique_sales),
round((cast(sum(unique_earlyticket) as double)/cast(sum(unique_sales) as double)),4)*100 as Early_Tickets_KPI,
round((cast(sum(unique_longinstall) as double)/cast(sum(unique_sales) as double)),4)*100 as Long_Installs_KPI,
round((cast(sum(unique_mrcchange) as double)/cast(sum(noplan) as double)),4)*100 as MRC_Changes_KPI,
--sum(unique_mrcchange) as mrcchange,
--sum(noplan) as noplan,
round((cast(sum(unique_mountingbill) as double)/cast(sum(activebase) as double)),4)*100 as Mounting_Bills_KPI,
round((cast(sum(unique_neverpaid) as double)/cast(sum(unique_sales) as double)),4)*100 as Never_Paid_KPI,
round((cast(sum(Unique_BillClaim) as double)/cast(sum(activebase) as double)),4)*100 as bill_claims_KPIS,
round((cast(sum(Unique_EarlyInteraction) as double)/cast(sum(unique_sales) as double)),4)*100 as New_Customer_Callers
from "lla_cco_int_ana_dev"."cwp_operational_drivers_dev"
where month = date (dt)
group by 1 
order by 1
