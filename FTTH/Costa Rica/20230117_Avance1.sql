-------------------------------------------------------------------------------------------------------------
----------------------------------- FTTH - COSTA RICA -------------------------------------------------------
-------------------------------------------------------------------------------------------------------------

WITH

base_proyectos AS (
SELECT  DATE_TRUNC('MONTH', date(DATE_PARSE(month,'%d/%m/%Y'))) AS cohort_month
        ,provincia
        ,distrito
        ,upper(trim(replace("nodo/fdh",'-'))) as nodo
        ,trim(olt) as olt
        ,tipo
        ,homepassed_b2c as home_passed
        ,cast(costo_total_proyecto as bigint) as costo
FROM "lla_cco_int_san"."cr_ftth_projects"
WHERE "hfc/ftth" = 'FTTH'
)

,base_proyectos_acum AS (
SELECT *, date('2022-01-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-01-01')
UNION ALL
SELECT *, date('2022-02-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-02-01')
UNION ALL
SELECT *, date('2022-03-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-03-01')
UNION ALL
SELECT *, date('2022-04-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-04-01')
UNION ALL
SELECT *, date('2022-05-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-05-01')
UNION ALL
SELECT *, date('2022-06-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-06-01')
UNION ALL
SELECT *, date('2022-07-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-07-01')
UNION ALL
SELECT *, date('2022-08-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-08-01')
UNION ALL
SELECT *, date('2022-09-01') as month FROM base_proyectos WHERE cohort_month <= date('2022-09-01')
)

select  month
        ,cohort_month
        ,-date_diff('month',month,cohort_month) as month_index
        ,provincia
        ,distrito
        ,nodo
        ,olt
        ,tipo
        ,sum(home_passed) as home_passed
        ,sum(costo) as costo
        --,round(cast(sum(costo) as double)/cast(sum(home_passed) as double),2) as cost_per_homepassed
from base_proyectos_acum
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8
