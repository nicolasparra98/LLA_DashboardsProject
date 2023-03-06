WITH

parameters AS (
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
        ,85 as overdue_days
)

,fixed_base AS (
SELECT  date(dt) AS dt
        ,sub_acct_no_sbb AS fixed_account
        ,home_phone_sbb AS fixed_phone1
        ,bus_phone_sbb AS fixed_phone2
        ,IF(welcome_offer = 'X' AND joint_customer = 'X','1.Real FMC',IF(welcome_offer IS NULL AND joint_customer = 'X','2.Near FMC','3.Fixed Only')) AS OPCO_FMC_Flag
        ,bill_code
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr"
WHERE play_type <> '0P'
    AND cust_typ_sbb = 'RES' 
    AND date(dt) = (SELECT input_month FROM parameters)
    AND (CAST(delinquency_days AS INTEGER) < (SELECT overdue_days FROM parameters) OR delinquency_days IS NULL)
)

,postpaid AS (
SELECT  subsrptn_id AS mobile_account
        ,cust_id AS parent_account
        ,'Postpaid' as mobile_type
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,prepaid AS (
SELECT  subsrptn_id AS mobile_account
        ,cust_id AS parent_account
        ,'Postpaid' as mobile_type
FROM "lcpr.stage.dev"."tbl_prepd_erc_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month 
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND ba_rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,second_step AS (
select  distinct *
        ,if(fixed_account is null,cast(mobile_account as varchar),concat(cast(fixed_account as varchar),' - ',cast(mobile_account as varchar))) as final_account
        ,row_number() over (partition by mobile_account order by fixed_account desc) as row_num_general
from(
SELECT A.*,B.fixed_account,'FMC' as Convergency
FROM (SELECT * FROM PREPAID UNION ALL SELECT * FROM POSTPAID) A INNER JOIN FIXED_BASE B ON MOBILE_ACCOUNT = fixed_phone1
UNION ALL
SELECT A.*,B.fixed_account,'FMC' as Convergency
FROM (SELECT * FROM PREPAID UNION ALL SELECT * FROM POSTPAID) A INNER JOIN FIXED_BASE B ON MOBILE_ACCOUNT = fixed_phone2
UNION ALL
SELECT *, NULL AS fixed_account, 'Mobile Only' as Convergency
FROM (SELECT * FROM PREPAID UNION ALL SELECT * FROM POSTPAID)
WHERE MOBILE_ACCOUNT NOT IN (SELECT DISTINCT fixed_phone1 FROM FIXED_BASE)
    AND MOBILE_ACCOUNT NOT IN (SELECT DISTINCT fixed_phone2 FROM FIXED_BASE)
))


SELECT *--row_num_general, count(distinct mobile_account) as mobile_account 
FROM SECOND_STEP 
where row_num_general = 19
--group by 1
--order by 1
