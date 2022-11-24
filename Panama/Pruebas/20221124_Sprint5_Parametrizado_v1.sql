-----------------------------------------------------------------------------------------
------------------------- SPRINT 5 PARAMETRIZADO - V1 -----------------------------------
-----------------------------------------------------------------------------------------

WITH 

parameters as(
select 
date_trunc('month',date('2022-01-01')) as input_month
)

,FMC_Table AS(
select distinct month, B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,finalchurnflag,fixedchurntype,fixedchurnflag,fixedmainmovement,waterfall_flag,finalaccount,fixedaccount,f_activebom,mobile_activeeom,mobilechurnflag
  FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
  --"lla_cco_int_stg"."cwp_sp3_basekpis_dashboardinput_dinamico_rj_v3"
  where month=date(dt)
  and month = (SELECT input_month FROM PARAMETERS)
)

,Repeated_Accounts AS(
select distinct month,fixedaccount,count(*) as RecordsPerUser
from FMC_Table
group by 1,2
)

,FMC_Table_Adj AS(
select distinct f.*,RecordsPerUser
from FMC_Table f LEFT JOIN Repeated_Accounts r ON f.fixedaccount=r.fixedaccount AND f.month=r.month
)
---------------Interactions Table Fields---------------------------------
,clean_interaction_time as (
select distinct *
from "db-stage-prod"."interactions_cwp"
    WHERE (cast(INTERACTION_START_TIME as varchar) != ' ') AND(INTERACTION_START_TIME IS NOT NULL)
    and DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) between ((SELECT input_month FROM PARAMETERS)) and ((SELECT input_month FROM PARAMETERS) + interval '1' month)
    --AND INTERACTION_ID NOT LIKE '%-%'
)
,interactions_fields as (
select distinct *,CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE) AS interaction_date, DATE_TRUNC('month',CAST(SUBSTR(cast(INTERACTION_START_TIME as varchar),1,10) AS DATE)) AS month
FROM clean_interaction_time
)
---------------------Users with Interactions (Tiers)--------------------------
,Last_Interaction as(
Select distinct account_id as last_account
,first_value(interaction_date) over(partition by account_id,date_trunc('month',interaction_date) order by interaction_date desc) as last_interaction_date
From interactions_fields
)
,Join_Last_Interaction as(
select distinct account_id,interaction_id,interaction_date,date_trunc('month',last_interaction_date) as InteractionMonth,last_interaction_date,date_add('DAY',-60, last_interaction_date) as window_day
from interactions_fields w inner join Last_Interaction l on w.account_id=l.last_account
)
,Interactions_Count as (
select distinct InteractionMonth,account_id,count(distinct interaction_id) as Interactions
from Join_Last_Interaction
where interaction_date between window_day and last_interaction_date
group by 1,2
)
,Interactions_Tier as (
select distinct i.*,
case when Interactions = 1 THEN '1' 
     when Interactions = 2 THEN '2' 
     when Interactions >= 3 THEN '>3'
else null end as InteractionsTier
FROM Interactions_Count i 
)
---------------------Users with Tickets (Tiers)--------------------------
,Users_Tickets as (
select distinct ACCOUNT_ID, interaction_id, INTERACTION_DATE
FROM interactions_fields
where interaction_purpose_descrip = 'TICKET'
) 
,Last_Ticket as(
Select distinct account_id as last_account
,first_value(interaction_date) over(partition by account_id,date_trunc('month',interaction_date) order by interaction_date desc) as last_interaction_date
From Users_Tickets
)
,Join_Last_Ticket as(
select distinct account_id,interaction_id,interaction_date,date_trunc('month',last_interaction_date) as InteractionMonth,last_interaction_date,date_add('DAY',-60, last_interaction_date) as window_day
from Users_Tickets w inner join Last_Ticket l on w.account_id=l.last_account
)
,Tickets_Count as (
select distinct InteractionMonth,account_id,count(distinct interaction_id) as Tickets
from Join_Last_Ticket
where interaction_date between window_day and last_interaction_date
group by 1,2
)
,Tickets_Tier as (
select distinct i.*,
case when Tickets = 1 THEN '1' 
     when Tickets = 2 THEN '2' 
     when Tickets >= 3 THEN '>3'
else null end as TicketsTier
FROM Tickets_Count i 
)
------------------Tickets per month-----------------------
,Tickets_per_month AS(
select distinct date_trunc('month',interaction_date) as month,account_id,count(interaction_date) AS NumberTickets
from Users_Tickets
where interaction_id is not null
group by 1,2
)
------------------Repair Times----------------------------
,Repair_times as (
select distinct account_id, cast(substr(cast(interaction_start_time as varchar),1,10) as date) as interaction_start_time, cast(substr(cast(interaction_end_time as varchar),1,10) as date) as interaction_end_time, DATE_DIFF('day',cast(substr(cast(interaction_start_time as varchar),1,10) as date), cast(substr(cast(interaction_end_time as varchar),1,10) as date)) AS Duration, DATE_TRUNC ('Month',cast(substr(cast(interaction_start_time as varchar),1,10) as date)) AS Month
FROM clean_interaction_time
WHERE interaction_purpose_descrip = 'TICKET' AND interaction_status ='CLOSED'
)
-----------------Missed Visits------------------------------
,Missed_Visits AS(
select distinct month,account_id
,case when other_interaction_info8 IN('Cliente reagenda cita','Cliente ausente','Cliente no deja entrar') then account_id else null end as missedvisits
from interactions_fields
where interaction_purpose_descrip = 'TRUCKROLL' and interaction_status ='CLOSED'
)
-----------------Sprint 5 Flags-----------------------------
,InteractionsTier_Flag AS(
select distinct f.*
,case when i.account_id is not null then finalaccount else null end as Interactions
,InteractionsTier
from fmc_table_adj f left join interactions_tier i ON f.finalaccount=i.account_id AND f.month=i.InteractionMonth
)
,TicketsTier_Flag AS(
select distinct f.*
,case when i.account_id is not null then finalaccount else null end as Tickets
,TicketsTier
from InteractionsTier_Flag f left join tickets_tier i ON f.fixedaccount=i.account_id AND f.month=i.InteractionMonth
)
,NumberTickets_Flag AS(
select distinct f.*
,NumberTickets
from TicketsTier_Flag f left join tickets_per_month i ON f.fixedaccount=i.account_id AND f.month=i.Month
)
,RepairTimes_Flag AS(
select distinct f.*
,case when duration>=4 then finalaccount else null end as OutlierRepair
from NumberTickets_Flag f left join Repair_times r ON f.finalaccount=r.account_id AND f.month=r.month
)
,MissedVisits_Flag AS(
select distinct f.*
,case when m.account_id is not null then finalaccount else null end as UsersTruckRolls
,missedvisits
from RepairTimes_Flag f left join Missed_visits m ON f.finalaccount=m.account_id AND f.month=m.month

)
,Final_Fields AS(
select distinct month, B_Final_TechFlag, B_FMCSegment, B_FMCType
,E_Final_TechFlag, E_FMCSegment,E_FMCType
,b_final_tenure
,e_final_tenure
,B_FixedTenure
,E_FixedTenure,finalchurnflag,fixedchurnflag,fixedchurntype
,fixedmainmovement,waterfall_flag,mobile_activeeom,mobilechurnflag
,InteractionsTier,TicketsTier,finalaccount,fixedaccount,interactions,tickets,numbertickets as PrevNumberTickets,recordsperuser,numbertickets--/RecordsPerUser 
as NumberTickets,OutlierRepair,UsersTruckrolls,Missedvisits
from MissedVisits_Flag
)
--/*
select distinct month
,B_Final_TechFlag, B_FMCSegment, B_FMCType,E_Final_TechFlag, E_FMCSegment, E_FMCType,b_final_tenure,e_final_tenure,B_FixedTenure,E_FixedTenure,InteractionsTier,TicketsTier,finalchurnflag,fixedchurnflag,waterfall_flag
,count(distinct finalaccount) as Total_Accounts
, count(distinct fixedaccount) as Fixed_Accounts
,count(distinct interactions) as UsersInteractions,count(distinct tickets) as UsersTickets, round(sum(NumberTickets),0) as NumberTickets
,count(distinct OutlierRepair) as OutlierRepairs
,count(distinct userstruckrolls) as UsersTruckrolls,count(distinct missedvisits) as MissedVisits
from final_fields
--where month=date('2022-02-01')
WHERE ((Fixedchurntype != 'Fixed Voluntary Churner' and Fixedchurntype != 'Fixed Involuntary Churner') or  Fixedchurntype is null) and finalchurnflag !='Fixed Churner'
--and month=date('2022-02-01') and tickets is not null and e_fmcsegment is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--order by 1--,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
