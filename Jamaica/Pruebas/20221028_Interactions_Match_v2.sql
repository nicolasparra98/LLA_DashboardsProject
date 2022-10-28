WITH 

initial_table as(
SELECT date_trunc('Month', date(interaction_start_time)) as Ticket_Month, account_id_2 as account, 
last_value(interaction_start_time) over (partition by account_id_2, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt, *
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%')
)

,cerillion_interactions as (
select Ticket_Month, account, last_int_dt, count(distinct interaction_id) as tickets
from initial_table
where partition_0 = 'cerillion' and ticket_month >= date ('2022-01-01')
group by 1,2,3
)

,acut_interactions as (
select Ticket_Month, account, last_int_dt, count(distinct interaction_id) as tickets
from initial_table
where partition_0 = 'acut' and ticket_month >= date ('2022-01-01')
group by 1,2,3
)

,repeated_users as (
select A.ticket_month as month_CER, A.account as account_CER, A.last_int_dt as interaction_start_time_CER, A.tickets as tickets_CER,
B.ticket_month as month_ACUT, B.account as account_ACUT, B.last_int_dt as interaction_start_time_ACUT, b.tickets as tickets_CER,
case when A.account = B.account then 1 else 0 end as repeated_user
from cerillion_interactions A full outer join acut_interactions B on a.ticket_month = b.ticket_month and a.account = b.account --and A.interaction_id = B.interaction_id
)

,grouping_table as (
select *, case when interaction_start_time_CER=interaction_start_time_acut then 1 else 0 end as repeated_tickets
from repeated_users
where repeated_user=1
)

select distinct month_CER, sum(repeated_tickets)
from grouping_table
group by 1
