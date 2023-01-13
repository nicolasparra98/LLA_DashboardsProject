WITH
--> BASE DE PROYECTOS CR
cr_projects as (
SELECT  distinct month
        ,tipo
        ,trim(olt) as olt
        ,sum(homepassed_b2c) as homepassed
        ,cast(sum(costo_total_proyecto) as bigint) as costo
FROM "lla_cco_int_san"."cr_ftth_projects" 
where "hfc/ftth" = 'FTTH'
group by 1,2,3
order by 3,2,1
)

/*
select distinct trim(olt) 
FROM "lla_cco_int_san"."cr_ftth_projects" 
where "hfc/ftth" = 'FTTH'
*/

--> BASE DE CUENTAS CR
,cr_accounts as (
SELECT DISTINCT trim(OLT) as olt, COUNT(DISTINCT ACCOUNT_ID) as cuentas 
FROM    (SELECT  col0 as OLT
                ,col1 as account_id
        FROM "lla_cco_int_san"."cr_ftth_base_cuentas"
        where col0 <> 'OLT'
        )
GROUP BY 1
ORDER BY 1
)

,join_bases as (
select A.*, B.cuentas
from cr_projects A left join cr_accounts B
on A.olt = B.olt
)

select *
        ,round((cast(cuentas as double)*100/cast(homepassed as double)),2) as penetracion
from join_bases 
