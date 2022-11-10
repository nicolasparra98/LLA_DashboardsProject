,prueba as(
select *, row_number() OVER (PARTITION BY REGEXP_REPLACE(account_id,'[^0-9 ]',''), cast(interaction_start_time as date) ORDER BY interaction_start_time desc) as row_num
from "db-stage-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%'
)


-- esto se usa donde la base de interactions se este llamando
select * from prueba having row_num = 1
