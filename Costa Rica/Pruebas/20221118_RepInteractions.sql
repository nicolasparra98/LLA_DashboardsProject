WITH

prueba AS(
SELECT *, row_number() OVER (PARTITION BY account_id, cast(interaction_start_time AS DATE),interaction_purpose_descrip ORDER BY interaction_start_time DESC) AS row_num
FROM "db-stage-dev"."interactions_cabletica"
WHERE account_type='RESIDENCIAL' --or account_type='PROGRAMA HOGARES CONECTADOS') 
AND date_trunc('Month',interaction_start_time)>=DATE('2022-01-01')-- and interaction_status <> 'ANULADA'
)

,sample AS(
SELECT *, CASE WHEN concat(account_id,cast(DATE(interaction_start_time) AS VARCHAR),interaction_purpose_descrip) IN (SELECT DISTINCT concat(account_id,cast(DATE(interaction_start_time) AS VARCHAR),interaction_purpose_descrip) FROM prueba WHERE row_num >=2) then 1 else 0 end AS rep_interact
FROM prueba
WHERE Date_Trunc('Month',DATE(interaction_start_time)) = DATE('2022-10-01')
ORDER BY account_id, cast(interaction_start_time AS DATE),row_num
)

--select  * FROM sample HAVING rep_interact=1 LIMIT 10000


SELECT Date_Trunc('Month',date(interaction_start_time)) as month, count(distinct account_id) as num_accounts, count (distinct interaction_id) as num_interactions
--FROM "db-stage-dev"."interactions_cabletica"
FROM (select * from prueba having row_num = 1)
WHERE (account_type='RESIDENCIAL' or account_type='PROGRAMA HOGARES CONECTADOS') and date_trunc('Month',interaction_start_time)>=date('2022-01-01')-- and interaction_status <> 'ANULADA'
group by 1
order by 1
