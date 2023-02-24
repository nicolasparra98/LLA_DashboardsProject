-----------------------------------------------------------------------------------------
----------------------------- LCPR FMC TABLE - V2 ---------------------------------------
-----------------------------------------------------------------------------------------
--CREATE TABLE IF NOT EXISTS "db_stage_dev"."lcpr_mobile_table_jan_feb23" AS

WITH

parameters AS (
--> Seleccionar el mes en que se desea realizar la corrida
SELECT  DATE_TRUNC('month',DATE('2023-01-01')) AS input_month
        ,85 as overdue_days
)

,fixed_table AS (
SELECT *
FROM "db_stage_dev"."lcpr_fixed_table_jan_feb23"
WHERE fix_s_dim_month = (SELECT input_month FROM parameters)
)

,mobile_table AS (
SELECT *
FROM "db_stage_dev"."lcpr_mobile_table_jan_feb23_v2"
WHERE mob_s_dim_month = (SELECT input_month FROM parameters)
)

,convergency AS (
SELECT  *
        ,row_number() OVER (PARTITION BY mobile_account ORDER BY fixed_account desc) as row_num
        ,row_number() OVER (PARTITION BY fixed_account ORDER BY mobile_account desc) as row_num2
FROM    (
        SELECT  fix_s_dim_month AS month
                ,fix_s_att_account AS fixed_account
                ,mob_s_att_account AS mobile_account
        FROM    (SELECT fix_s_dim_month,fix_s_att_account,fix_s_att_phone1 FROM fixed_table) A
                    INNER JOIN
                (SELECT mob_s_dim_month,mob_s_att_account FROM mobile_table) B
                    ON A.fix_s_dim_month = B.mob_s_dim_month AND A.fix_s_att_phone1 = B.mob_s_att_account
        UNION ALL
        SELECT  fix_s_dim_month AS month
                ,fix_s_att_account AS fixed_account
                ,mob_s_att_account AS mobile_account
        FROM    (SELECT fix_s_dim_month,fix_s_att_account,fix_s_att_phone2 FROM fixed_table) A
                    INNER JOIN
                (SELECT mob_s_dim_month,mob_s_att_account FROM mobile_table) B
                    ON A.fix_s_dim_month = B.mob_s_dim_month AND A.fix_s_att_phone2 = B.mob_s_att_account
        )
)

,FMC_base AS (
SELECT  IF(fix_s_dim_month IS NOT NULL,fix_s_dim_month,mob_s_dim_month) AS fmc_s_dim_month
        ,IF(fix_s_att_account IS NOT NULL AND mob_s_att_account IS NOT NULL,CONCAT(CAST(fix_s_att_account AS VARCHAR),' - ',CAST(mob_s_att_account AS VARCHAR(10))),IF(mob_s_att_account IS NULL,CAST(fix_s_att_account AS VARCHAR),CAST(mob_s_att_account AS VARCHAR(10)))) AS fmc_s_att_account
        ,IF(IF(fix_b_att_active IS NULL,0,fix_b_att_active) + IF(mob_b_att_active IS NULL,0,mob_b_att_active) >= 1,1,0) AS fmc_b_att_active
        ,IF(IF(fix_e_att_active IS NULL,0,fix_e_att_active) + IF(mob_e_att_active IS NULL,0,mob_e_att_active) >= 1,1,0) AS fmc_e_att_active
        ,fix_s_att_account
        ,fix_b_att_active
        ,fix_e_att_active
        ,fix_s_att_phone1
        ,fix_s_att_phone2
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_MaxStart
        ,fix_b_fla_tenure
        ,fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_fla_fmc
        ,fix_b_mes_numRGUS
        ,fix_b_fla_MixNameAdj
        ,fix_b_fla_MixCodeAdj
        ,fix_b_fla_BB
        ,fix_b_fla_TV
        ,fix_b_fla_VO
        ,fix_b_att_bbCode
        ,fix_b_att_tvCode
        ,fix_b_att_voCode
        ,fix_b_fla_subsidized
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_MaxStart
        ,fix_e_fla_tenure
        ,fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_fla_fmc
        ,fix_e_mes_numRGUS
        ,fix_e_fla_MixNameAdj
        ,fix_e_fla_MixCodeAdj
        ,fix_e_fla_BB
        ,fix_e_fla_TV
        ,fix_e_fla_VO
        ,fix_e_att_bbCode
        ,fix_e_att_tvCode
        ,fix_e_att_voCode
        ,fix_e_fla_subsidized
        ,fix_s_fla_MainMovement
        ,fix_s_fla_SpinMovement
        ,fix_s_fla_ChurnFlag
        ,fix_s_fla_ChurnType
        ,fix_s_fla_ChurnSubType
        ,fix_s_fla_Rejoiner
        ,mob_s_att_account
        ,mob_s_att_parentaccount
        ,mob_b_att_active
        ,mob_e_att_active
        ,mob_b_dim_date
        ,mob_b_mes_tenuredays
        ,mob_b_att_maxstart
        ,mob_b_fla_tenure
        ,mob_b_mes_mrc
        ,mob_b_mes_numrgus
        ,mob_e_dim_date
        ,mob_e_mes_tenuredays
        ,mob_e_att_maxstart
        ,mob_e_fla_tenure
        ,mob_e_mes_mrc
        ,mob_e_mes_numrgus
        ,mob_s_fla_mainmovement
        ,mob_s_mes_mrcdiff
        ,mob_s_fla_spinmovement
        ,mob_s_fla_churnflag
        ,mob_s_fla_churntype
        ,CASE   WHEN (fix_b_fla_tenure IS NOT NULL AND mob_b_fla_tenure IS NULL) THEN fix_b_fla_tenure
                WHEN (fix_b_fla_tenure = mob_b_fla_tenure) THEN fix_b_fla_tenure
                WHEN (mob_b_fla_tenure IS NOT NULL AND fix_b_fla_tenure IS NULL) THEN mob_b_fla_tenure
                WHEN (fix_b_fla_tenure <> mob_b_fla_tenure AND (fix_b_fla_tenure = 'Early Tenure'  or mob_b_fla_tenure = 'Early Tenure' )) THEN 'Early-Tenure'
                    END AS fmc_b_fla_tenure
        ,CASE   WHEN (fix_e_fla_tenure IS NOT NULL AND mob_e_fla_tenure IS NULL) THEN fix_e_fla_tenure
                WHEN (fix_e_fla_tenure = mob_e_fla_tenure) THEN fix_e_fla_tenure
                WHEN (mob_e_fla_tenure IS NOT NULL AND fix_e_fla_tenure IS NULL) THEN mob_e_fla_tenure
                WHEN (fix_e_fla_tenure <> mob_e_fla_tenure AND (fix_e_fla_tenure = 'Early Tenure'  or mob_e_fla_tenure = 'Early Tenure' )) THEN 'Early-Tenure'
                    END AS fmc_e_fla_tenure
        ,IF(fix_b_mes_numRGUS IS NULL,0,fix_b_mes_numRGUS) + IF(mob_b_mes_numRGUS IS NULL,0,mob_b_mes_numRGUS) AS fmc_b_mes_numRGUS
        ,IF(fix_e_mes_numRGUS IS NULL,0,fix_e_mes_numRGUS) + IF(mob_e_mes_numRGUS IS NULL,0,mob_e_mes_numRGUS) AS fmc_e_mes_numRGUS
        ,IF(fix_b_mes_mrc IS NULL,0,fix_b_mes_mrc) + IF(mob_b_mes_mrc IS NULL,0,mob_b_mes_mrc) AS fmc_b_mes_mrc
        ,IF(fix_e_mes_mrc IS NULL,0,fix_e_mes_mrc) + IF(mob_e_mes_mrc IS NULL,0,mob_e_mes_mrc) AS fmc_e_mes_mrc
        
FROM    (
        SELECT A.*, B.mobile_account
        FROM fixed_table A LEFT JOIN (SELECT * FROM convergency WHERE row_num = 1 AND row_num2 = 1) B
            ON A.fix_s_att_account = B.fixed_account AND A.fix_s_dim_month = B.month
        WHERE fix_s_att_account IS NOT NULL
        ) C FULL OUTER JOIN mobile_table D
    ON C.mobile_account = D.mob_s_att_account AND C.fix_s_dim_month = D.mob_s_dim_month
)

select * from FMC_base 
where mob_s_att_account is not null --and fix_s_att_account is not null
limit 100
