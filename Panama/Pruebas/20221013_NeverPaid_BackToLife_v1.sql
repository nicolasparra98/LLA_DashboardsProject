WITH

FMC_Table as(
SELECT * --, FIRST_VALUE(month) OVER (PARTITION BY finalaccount ORDER BY month) AS first_month
FROM "lla_cco_int_ana_prod"."cwp_fmc_churn_prod"
where Month=date(dt) and fixedmainmovement in ('5.Come Back to Life','4.New Customer') 
)

,union_dna as (
    select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,act_cust_typ_nm,date_trunc('month',date(dt)) as Month_load,fi_bill_dt_m0,fi_bill_dt_m1,fi_bill_due_dt_m1,fi_bill_due_dt_m0,fi_bill_dt_m2,fi_bill_due_dt_m2
    from "db-analytics-prod"."fixed_cwp"
    where act_cust_typ_nm = 'Residencial'
    and (cast(fi_outst_age as bigint) <= 95 or fi_outst_age is null)
)
,monthly_inst_accounts as (
select distinct act_acct_cd,DATE_TRUNC('month',date(act_acct_inst_dt)) as InstMonth
from union_dna
WHERE act_cust_typ_nm = 'Residencial' and DATE_TRUNC('month',date(act_acct_inst_dt)) = month_load
)
,first_bill as(
SELECT distinct act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill,date_trunc('month',first_inst_dt) as instmonth
 FROM(select act_acct_cd,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt
    from (select act_acct_cd, fi_outst_age, date(dt) as dt,act_acct_inst_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
        and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
        AND date(dt) between ((DATE_TRUNC('month',date(act_cust_strt_dt))) - interval '12' month) and ((DATE_TRUNC('month',date(act_cust_strt_dt))) + interval '6' month) )
  where oldest_unpaid_bill_dt <> '1900-01-01' )
 group by act_acct_cd,3
)
,max_overdue_first_bill as (
select act_acct_cd, DATE_TRUNC('month',date(min(first_inst_dt))) as Month_Inst,
min(date(first_oldest_unpaid_bill_dt)) as first_oldest_unpaid_bill_dt,
min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
max(fi_outst_age) as max_fi_outst_age, 
max(fi_overdue_age) as max_fi_overdue_age,
max(date(dt)) as max_dt,
case when max(cast(fi_outst_age as int))>=(90) then 1 else 0 end as hard_dx_flg
FROM (select act_acct_cd,
    FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_oldest_unpaid_bill_dt,
    FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_inst_dt, 
    FIRST_VALUE(date(act_cust_strt_dt)) OVER (PARTITION BY act_acct_cd ORDER BY dt) AS first_act_cust_strt_dt,
    fi_outst_age, date(dt) as dt, pd_mix_cd,fi_overdue_age
    FROM ( select act_acct_cd, fi_outst_age, date(dt) as dt,pd_mix_cd,pd_bb_accs_media,pd_TV_accs_media,pd_VO_accs_media, act_acct_inst_dt,act_cust_strt_dt,
        case when fi_outst_age is null then '1900-01-01' else cast(date_add('day',-cast(fi_outst_age as int),date(dt)) as varchar) end as oldest_unpaid_bill_dt
        ,Case when fi_bill_dt_m0 is not null then cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m0),  date(fi_bill_due_dt_m0))
   when fi_bill_dt_m1 is not null then cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m1),  date(fi_bill_due_dt_m1))
   else cast(fi_outst_age as int) - date_diff('day', date(fi_bill_dt_m2),  date(fi_bill_due_dt_m2)) end as fi_overdue_age
        from union_dna
         WHERE act_cust_typ_nm = 'Residencial'
         and act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
         AND date(dt) between (DATE_TRUNC('month',date(act_acct_inst_dt))) and ((DATE_TRUNC('month',date(act_acct_inst_dt))) + interval '5' month) )
    where concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill) )
group by act_acct_cd 
)
,sft_hard_dx as(
select *, 
date_add('day',(46),first_oldest_unpaid_bill_dt) as threshold_pay_date,
case when (max_fi_outst_age>=46 and Month_Inst <date('2022-04-01')) or(max_fi_overdue_age>=5 and Month_Inst>=date('2022-04-01')) then 1 else 0 end as soft_dx_flg,
case when date_add('day',(46),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as soft_dx_window_completed,
case when date_add('day',(90),first_oldest_unpaid_bill_dt)  < current_date then 1 else 0 end as never_paid_window_completed,
current_date as current_date_analysis
from max_overdue_first_bill
)

,Join_dx_selected_customers as (
SELECT a.*, soft_dx_flg as soft_dx, hard_dx_flg as hard_dx
FROM --(select * from FMC_Table where month = first_month) AS a
FMC_Table as a
LEFT JOIN sft_hard_dx AS b ON a.finalaccount = b.act_acct_cd 
)

select distinct month, --first_month, 
fixedmainmovement, sum(hard_dx),count(distinct finalaccount)
from Join_dx_selected_customers
--where month = first_month
group by 1,2
order by 2,1
