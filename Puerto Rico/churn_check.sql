WITH 

parameters AS (
-- Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE('2023-01-01') AS opening_date
        ,DATE('2023-01-31') AS closing_date
        ,85 as overdue_days
)

,useful_fields AS (
SELECT  date(dt) AS dt
        ,DATE(as_of) AS as_of
        ,sub_acct_no_sbb AS fix_s_att_account
        ,home_phone_sbb AS fix_s_att_phone
        ,delinquency_days AS overdue
        ,(CAST(CAST(first_value(connect_dte_sbb) over (PARTITION BY sub_acct_no_sbb order by DATE(dt) DESC) AS TIMESTAMP) AS DATE)) AS max_start
        ,(video_chrg + hsd_chrg + voice_chrg) AS TOTAL_MRC
        ,IF(TRIM(drop_type) = 'FIBER','FTTH',IF(TRIM(drop_type) = 'FIBCO','HFC',IF(TRIM(drop_type) = 'COAX','COPPER',null))) AS tech_flag
        ,hsd AS numBB
        ,video AS numTV
        ,voice AS numVO
        ,null AS oldest_unpaid_bill_dt
        ,first_value(delinquency_days) over(PARTITION BY sub_acct_no_sbb,date_trunc('month',date(dt)) ORDER BY date(dt) DESC) AS last_overdue
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) BETWEEN (SELECT opening_date FROM parameters) AND (SELECT closing_date FROM parameters) 
)

,active_base AS (
select distinct fix_s_att_account
from useful_fields
WHERE dt = (SELECT opening_date FROM parameters)
    AND (CAST(overdue AS INTEGER) <= (SELECT overdue_days FROM parameters) OR overdue IS NULL)
)

,candidates AS (
SELECT distinct fix_s_att_account as cuentas, max(overdue) as max_overdue
FROM useful_fields
WHERE last_overdue >= (SELECT overdue_days FROM parameters) and fix_s_att_account in (select distinct fix_s_att_account from active_base)
group by 1
)

,involuntary_churners as (
SELECT fix_s_att_account as cuentas, (numBB + numTV + numVO) as RGUs
from useful_fields
where dt = (SELECT opening_date FROM parameters) and fix_s_att_account in (select distinct cuentas from candidates WHERE max_overdue >= (SELECT overdue_days FROM parameters) )
)

select count(distinct cuentas), sum(RGUs)
from involuntary_churners
