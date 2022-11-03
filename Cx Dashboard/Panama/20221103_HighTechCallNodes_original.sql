-----------------------------------------
-- HOME INTEGRITY
-----------------------------------------

with 

home_integrity_node_base as (
select month,mac_address, max(replace(mac_address,':','')) as MAC_JOIN,max(first_node_name) as first_node_name, min(first_fecha_carga) as first_fecha_carga
from (select dATE_TRUNC('month',date(fecha_carga)) as month,mac_address, first_value(node_name) over(partition by mac_address,dATE_TRUNC('month',date(fecha_carga)) order by fecha_carga) as first_node_name,first_value(fecha_carga) over(partition by mac_address,dATE_TRUNC('month',date(fecha_carga)) order by fecha_carga) as first_fecha_carga
      from "db-stage-dev"."home_integrity_history" where node_name is not null) group by month,mac_address
)

,map_mac_account as (
select month_map_mac,act_acct_cd, max(first_nr_bb_mac) as nr_bb_mac, max(first_fi_outst_age) as first_fi_outst_age
    from ( select date_trunc('month',date(dt)) as month_map_mac,act_acct_cd, first_value(nr_bb_mac) over(partition by act_acct_cd,dATE_TRUNC('month',date(dt)) order by dt) as first_nr_bb_mac,first_value(fi_outst_age) over(partition by act_acct_cd,dATE_TRUNC('month',date(dt)) order by dt) as first_fi_outst_age
from "db-analytics-prod"."fixed_cwp" where date(dt) =date_trunc('month',date(dt)) and nr_bb_mac is not null )
where (cast(first_fi_outst_age as int) < (90) or first_fi_outst_age is null) group by 1,act_acct_cd
)
,join_account_id as (
select a.*,b.* from map_mac_account a
left join home_integrity_node_base  b on b.MAC_JOIN = a.nr_bb_mac and a.month_map_mac=b.month where  b.MAC_JOIN is not null 
)



,interactions_panel as (
select DATE_TRUNC('month',date(INTERACTION_START_TIME)) as inter_month,ACCOUNT_ID as interactions_account_id,count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) as num_total_claims,count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as num_tech_tickets,count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end) as num_tech_truckrolls,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'CLAIM' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as claims_flag,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) > 0 then 1 else 0 end as tickets_flag,case when count(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TRUCKROLL' then date(INTERACTION_START_TIME) end)  > 0 then 1 else 0 end as truckroll_flag,array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then date(INTERACTION_START_TIME) end) as list_dates_tickets,array_agg(distinct case when INTERACTION_PURPOSE_DESCRIP = 'TICKET' then interaction_id end) as list_interaction_id_tickets
    from "db-stage-prod"."interactions_cwp" where ACCOUNT_ID in (select act_acct_cd from join_account_id) and interaction_id is not null and INTERACTION_ID NOT LIKE '%-%' group by 1,ACCOUNT_ID
)

--select distinct inter_month from interactions_panel

,join_interactions as (
select a.*,b.* from join_account_id a 
left join interactions_panel b on a.act_acct_cd = b.interactions_account_id and a.month=b.inter_month
)
,group_node as (
select month,month_map_mac,hfc_node,accounts_with_tickets,total_accounts,accounts_with_tickets*100/total_accounts as percentage_accounts_with_tickets
from (select month,month_map_mac,first_node_name as hfc_node, cast(sum(tickets_flag) as double) as accounts_with_tickets,cast(count(distinct act_acct_cd) as double) as total_accounts from join_interactions group by 1,2,first_node_name)
)
,nodes_by_severity as (
select month,'CWP' as Opco,'Panama' as Market,'Large' as MarketSize,'Fixed' as Product,'B2C' as Biz_Unit,month_map_mac,total_nodes,nodes_higher_than_6_perc,nodes_higher_than_6_perc*100/total_nodes as kpi_percentage
from( select month,month_map_mac,cast(count(distinct hfc_node) as double) as total_nodes,cast(count(distinct case when percentage_accounts_with_tickets > 6 then hfc_node end) as double) as nodes_higher_than_6_perc
    from group_node group by month,2)
    )

select distinct month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'use' as journey_waypoint,'High_Tech_Call_Nodes_+6%Monthly' as kpi_name, round(kpi_percentage/100,4) as kpi_meas, nodes_higher_than_6_perc as kpi_num,total_nodes as kpi_den, 'M-0' as Kpi_delay_display,'OVERALL' as Network from nodes_by_severity
