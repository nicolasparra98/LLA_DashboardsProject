WITH 

cerillion_interactions as (
select date_trunc('Month', date(interaction_start_time)) as ticket_month,*
from (select *, REGEXP_REPLACE(account_id, '[^0-9 ]', '') as account
	    from "db-stage-dev"."interactions_cwc"
		where lower(org_cntry) like '%jam%')
where partition_0 = 'cerillion' and date_trunc('Month', date(interaction_start_time)) >= date ('2022-01-01')
)

,acut_interactions as (
select date_trunc('Month', date(interaction_start_time)) as ticket_month,*
from (select *, REGEXP_REPLACE(account_id, '[^0-9 ]', '') as account
		from "db-stage-dev"."interactions_cwc"
		where lower(org_cntry) like '%jam%')
where partition_0 = 'acut' and date_trunc('Month', date(interaction_start_time)) >= date ('2022-01-01')
)

,repeated_users as (
select A.ticket_month as month_CER, A.account as account_CER, A.interaction_start_time as interaction_start_time_CER,A.interaction_id as interac_id_CER,
B.ticket_month as month_ACUT, B.account as account_ACUT, B.interaction_start_time as interaction_start_time_ACUT,B.interaction_id as interac_id_CER,
case when A.account = B.account then 1 else 0 end as repeated_user,
case when A.interaction_id = B.interaction_id and A.account = B.account then 1 else 0 end as repeated_interaction
from cerillion_interactions A full outer join acut_interactions B on a.ticket_month = b.ticket_month and a.account_id = b.account_id --and A.interaction_start_time = B.interaction_start_time
)

select distinct month_cer, sum(repeated_interaction) from repeated_users
group by 1
order by 1


--select distinct month_cer, month_acut, count(distinct account_cer) as acc_cer, count(distinct account_acut) as acc_acut, sum(repeated_user) as rep_user, sum(repeated_interaction) as rep_interac
--from double_interactions
--where month_cer = date ('2022-09-01') and month_acut =date ('2022-09-01') and repeated_user=1
--group by 1,2
--order by 1,2

--select *, case when interaction_start_time_CER=interaction_start_time_acut then 1 else 0 end as repeated_interaction_flag
--from repeated_users
--where repeated_user=1                                              



 --select distinct account_CER, interaction_start_time_CER,interaction_start_time_acut, sum(repeated_user)
--from double_interactions
 --where month_cer = date ('2022-09-01') and month_acut =date ('2022-09-01') and repeated_user=1
--group by 1,2,3
--order by 1,2,3
