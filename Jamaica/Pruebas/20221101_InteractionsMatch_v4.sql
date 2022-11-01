WITH 

cerillion_interactions as (
select date_trunc('Month', date(interaction_start_time)) as ticket_month, cast(interaction_start_time as date) as interaction_day, *
from (select *, trim(REGEXP_REPLACE(account_id, '[^0-9 ]', '')) as account
	    from "db-stage-dev"."interactions_cwc" 
		where lower(org_cntry) like '%jam%')
where partition_0 = 'cerillion' --and date_trunc('Month', date(interaction_start_time)) >= date ('2022-01-01')
)

,acut_interactions as (
select date_trunc('Month', date(interaction_start_time)) as ticket_month, cast(interaction_start_time as date) as interaction_day, *
from (select *, trim(REGEXP_REPLACE(account_id, '[^0-9 ]', '')) as account
		from "db-stage-dev"."interactions_cwc"
		where lower(org_cntry) like '%jam%')
where partition_0 = 'acut' --and date_trunc('Month', date(interaction_start_time)) >= date ('2022-01-01')
)

,repeated_users as (
select B.*,
case when A.account = B.account then 1 else 0 end as repeated_user,
case when A.interaction_day = B.interaction_day  and A.account = B.account then 1 else 0 end as repeated_interaction
from cerillion_interactions A right join acut_interactions B on a.ticket_month = b.ticket_month and a.account_id = b.account_id --and A.interaction_start_time = B.interaction_start_time
)

--select distinct ticket_month, sum(repeated_user) as repeated_users,sum(repeated_interaction) as repeated_interactions from repeated_users
--where ticket_month >= date ('2022-01-01')
--group by 1
--order by 1

,interactions_union as (
select ticket_month, interaction_day,interaction_start_time, account, interaction_id,partition_0 from cerillion_interactions
union all
select * from (select ticket_month, interaction_day,interaction_start_time, account, interaction_id,partition_0 from repeated_users where repeated_interaction = 0)
)

,initial_table as (
select account as account2, concat(account,'-',cast(interaction_day as varchar)) as key,
last_value(interaction_start_time) over (partition by account, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt,* 
from interactions_union 
)

, tickets_count as (
SELECT Ticket_Month, account, 
case when (length(account) = 8) THEN 'Cerillion' else 'Liberate' end as CRM
,count(distinct key) as tickets
FROM initial_table
WHERE interaction_start_time between (last_int_dt - interval '60' day) and last_int_dt
GROUP BY 1,2
)


SELECT distinct Ticket_Month, count (distinct account) as cuentas, sum(tickets) as tickets
from tickets_count
where ticket_month >= date ('2022-01-01')
group by 1
order by 1

,reiterations_summary AS(

SELECT t.*, 
CASE WHEN tickets = 1 THEN account else null end as one_tckt,
CASE WHEN tickets > 1 THEN account else null end as over1_tckt,
CASE WHEN tickets = 2 THEN account else null end as two_tckt,
CASE WHEN tickets >= 3 THEN account else null end as three_tckt
FROM tickets_count t

)

--SELECT distinct Ticket_Month, count(distinct one_tckt),count(distinct over1_tckt),count (distinct two_tckt),count (distinct three_tckt),count (distinct account) as cuentas, sum(tickets) as tickets
--from reiterations_summary
--where ticket_month >= date ('2022-01-01')
--group by 1
--order by 1
