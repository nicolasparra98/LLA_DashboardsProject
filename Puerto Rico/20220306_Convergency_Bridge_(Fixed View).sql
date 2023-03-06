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
SELECT  subsrptn_id AS postpaid_account
        ,cust_id AS postpaid_parent_account
FROM "lcpr.stage.dev"."tbl_pstpd_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,prepaid AS (
SELECT  subsrptn_id AS prepaid_account
        ,cust_id AS prepaid_parent_account
FROM "lcpr.stage.dev"."tbl_prepd_erc_cust_mstr_ss_data"
WHERE date(dt) = (SELECT input_month FROM parameters) - interval '1' month 
    AND cust_sts = 'O'
    AND acct_type_cd = 'I'
    AND ba_rgn_nm <> 'VI'
    AND subsrptn_sts = 'A'
)

,FIRST_STEP AS (
SELECT  *
        ,IF(fixed_phone1 in (SELECT DISTINCT postpaid_account FROM postpaid) OR fixed_phone2 in (SELECT DISTINCT postpaid_account FROM postpaid),1,0) AS with_postpaid
        ,IF(fixed_phone1 in (SELECT DISTINCT prepaid_account FROM prepaid) OR fixed_phone2 in (SELECT DISTINCT prepaid_account FROM prepaid),1,0) AS with_prepaid
        ,if(bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),1,0) as with_FMC_package
FROM FIXED_BASE
)

/*
select  --OPCO_FMC_Flag,
        with_postpaid
        ,with_prepaid
        ,with_FMC_package
        ,count(distinct fixed_account) 
from first_step
group by 1,2,3--,4
*/

,second_step AS (
select  distinct *
        ,if(mobile_account is null,cast(fixed_account as varchar),concat(cast(fixed_account as varchar),' - ',cast(mobile_account as varchar))) as final_account
        ,row_number() over (partition by fixed_account order by mobile_account desc) as row_num_general
        ,row_number() over (partition by fixed_account,convergency order by mobile_account desc) as row_num_prepost
        ,row_number() ignore nulls over (partition by mobile_account order by fixed_account desc) as row_num_mobile
        ,CASE WHEN fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NULL THEN 'Fixed Only'
            WHEN fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NOT NULL AND Convergency = 'Postpaid' then if(bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Postpaid Real FMC',' Postpaid Near FMC')
            when fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NOT NULL AND Convergency = 'Prepaid' then if(bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Prepaid Real FMC',' Prepaid Near FMC') end as FMC_Flag_Oval
        ,CASE WHEN fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NULL THEN 'Fixed Only'
            WHEN  fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NOT NULL and with_postpaid = 0 and with_prepaid = 1 then if (bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Prepaid Real FMC','Prepaid Near FMC')
            WHEN  fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NOT NULL and with_postpaid = 1 and with_prepaid = 0 then if (bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Postpaid Real FMC','Postpaid Near FMC')
            WHEN  fixed_account IS NOT NULL AND MOBILE_ACCOUNT IS NOT NULL and with_postpaid = 1 and with_prepaid = 1 then if (bill_code IN (SELECT DISTINCT bill_code FROM "lcpr.stage.dev"."lcpr_fix_fmc_bill_codes"),'Real FMC','Near FMC') end as FMC_Flag_OvalAdj
from(
SELECT A.*,B.postpaid_account AS MOBILE_ACCOUNT, 'Postpaid' as convergency 
FROM first_step A INNER JOIN POSTPAID B ON fixed_phone1 = postpaid_account 
UNION ALL
SELECT A.*,B.postpaid_account AS MOBILE_ACCOUNT, 'Postpaid' as convergency 
FROM first_step A INNER JOIN POSTPAID B ON fixed_phone2 = postpaid_account 
UNION ALL
SELECT A.*,B.prepaid_account AS MOBILE_ACCOUNT, 'Prepaid' as convergency 
FROM first_step A INNER JOIN PREPAID B ON fixed_phone1 = prepaid_account 
UNION ALL
SELECT A.*,B.prepaid_account AS MOBILE_ACCOUNT, 'Prepaid' as convergency 
FROM first_step A INNER JOIN PREPAID B ON fixed_phone2 = prepaid_account 
UNION ALL
SELECT *, NULL AS mobile_account, 'Fixed Only' as convergency
from first_step
where  fixed_phone1 NOT IN (SELECT DISTINCT postpaid_account FROM POSTPAID)
    AND fixed_phone2 NOT IN (SELECT DISTINCT postpaid_account FROM POSTPAID)
    AND fixed_phone1 NOT IN (SELECT DISTINCT prepaid_account FROM prepaid)
    AND fixed_phone2 NOT IN (SELECT DISTINCT prepaid_account FROM prepaid)
))

/*
select convergency,count(distinct fixed_account) from second_step
group by 1
*/
/*
select convergency,row_num_prepost, count(distinct fixed_account)
from second_step
--where convergency = 'Postpaid'
group by 1,2
order by 1,2 asc
*/
/*
SELECT FMC_Flag_Oval,convergency,row_num_prepost, count(distinct fixed_account)
from second_step
group by 1,2,3
order by 1,2,3
*/
/*
select row_num_general,row_num_mobile, count(distinct fixed_account)
from second_step
where mobile_account is not null and row_num_mobile <=4
group by 1,2
order by 1,2 
*/

select  --OPCO_FMC_Flag
        --with_postpaid
        --,with_prepaid
        --,with_FMC_package
        FMC_Flag_Ovaladj
        ,count(distinct fixed_account) 
from second_step
group by 1--,2--,3,4--,5
order by 1--,2--,3,4
