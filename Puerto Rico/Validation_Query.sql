--------------------------------------------------------------------------------------------------
----------------------------- CHECK GENERAL DE FIJO ----------------------------------------------
--------------------------------------------------------------------------------------------------
/*
select  fmc_s_dim_month
        ,fix_b_att_active
        ,fix_e_att_active
        ,fix_b_fla_tenure
        ,fmc_b_fla_tenure
        ,fix_s_fla_MainMovement
        ,fix_s_fla_ChurnFlag
        ,fix_s_fla_ChurnType
        ,count(distinct fix_s_att_account) as accounts
        ,sum(fix_b_mes_numRGUS) as BOM_RGUS
        ,sum(fix_e_mes_numRGUS) as EOM_RGUS
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE fmc_s_dim_month = DATE(dt)
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1,4,5,6,7,8
*/
--------------------------------------------------------------------------------------------------
---------------------------- CHECK GENERAL DE MOBILE ---------------------------------------------
--------------------------------------------------------------------------------------------------
/*
select  fmc_s_dim_month
        ,mob_b_att_active
        ,mob_e_att_active
        ,mob_s_fla_MainMovement
        ,mob_s_fla_SpinMovement
        ,mob_s_fla_ChurnFlag
        ,mob_s_fla_ChurnType
        ,count(distinct mob_s_att_account) as accounts
        ,sum(mob_b_mes_numRGUS) as BOM_RGUS
        ,sum(mob_e_mes_numRGUS) as EOM_RGUS
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE fmc_s_dim_month = DATE(dt) and mob_s_att_duplicates = 1
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,4,5,6,7
*/
--------------------------------------------------------------------------------------------------
------------------------------ CHECK GENERAL DE FMC ----------------------------------------------
--------------------------------------------------------------------------------------------------
select  fmc_s_dim_month
        --,fmc_b_fla_fmc
        ,fmc_e_fla_fmc
        ,count(distinct fix_s_att_account) as accounts
        ,sum(fix_b_mes_numRGUS) as BOM_RGUS
        ,sum(fix_e_mes_numRGUS) as EOM_RGUS
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE fmc_s_dim_month = DATE(dt)
GROUP BY 1,2
ORDER BY 1,2
