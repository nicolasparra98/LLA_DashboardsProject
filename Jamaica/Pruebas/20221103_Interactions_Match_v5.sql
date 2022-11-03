clean_interactions_base AS(
select  * from "db-stage-dev"."interactions_cwc" where partition_0 = 'cerillion' 
union all
select  B.*
        from (select * from "db-stage-dev"."interactions_cwc" where partition_0 = 'cerillion') A
        right join 
             (select * from "db-stage-dev"."interactions_cwc" where partition_0 = 'acut') B
        on cast(A.interaction_start_time as date) = cast(B.interaction_start_time as date) and REGEXP_REPLACE(A.account_id,'[^0-9 ]','') = REGEXP_REPLACE(B.account_id,'[^0-9 ]','')
        where cast(A.interaction_start_time as date) is null and REGEXP_REPLACE(A.account_id,'[^0-9 ]','') is null
)
