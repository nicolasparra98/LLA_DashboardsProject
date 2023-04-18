-----------------------------------------------------------------------------------------
------------------------- FMC GROUPED FULL FLAGS - V1 -----------------------------------
-----------------------------------------------------------------------------------------

SELECT  fmc_s_dim_month
        ,fmc_b_att_active
        ,fmc_e_att_active
        ,fix_b_att_active
        ,fix_e_att_active
        ,fix_b_dim_date
        ,fix_b_mes_overdue
        ,fix_b_att_maxstart
        ,fix_b_fla_tenure
        ,CAST(ROUND(fix_b_mes_mrc,0) AS INT) AS fix_b_mes_mrc
        ,fix_b_fla_tech
        ,fix_b_mes_numrgus
        ,fix_b_fla_mixnameadj
        ,fix_b_fla_mixcodeadj
        ,fix_b_att_bbcode
        ,fix_b_att_tvcode
        ,fix_b_att_vocode
        ,fix_e_dim_date
        ,fix_e_mes_overdue
        ,fix_e_att_maxstart
        ,fix_e_fla_tenure
        ,CAST(ROUND(fix_e_mes_mrc,0) AS INT) AS fix_e_mes_mrc
        ,fix_e_fla_tech
        ,fix_e_mes_numrgus
        ,fix_e_fla_mixnameadj
        ,fix_e_fla_mixcodeadj
        ,fix_e_att_bbcode
        ,fix_e_att_tvcode
        ,fix_e_att_vocode
        ,fix_s_fla_mainmovement
        ,fix_s_fla_spinmovement
        ,fix_s_fla_churnflag
        ,fix_s_fla_churntype
        ,fix_s_fla_rejoiner
        ,COUNT(DISTINCT fix_s_att_phone1) AS fix_s_att_phone1
        ,COUNT(DISTINCT fix_s_att_phone2) AS fix_s_att_phone2
        ,fix_b_fla_fmc
        ,fix_e_fla_fmc
        ,fix_b_fla_subsidized
        ,fix_b_att_billcode
        ,fix_e_fla_subsidized
        ,fix_e_att_billcode
        ,fix_s_fla_churnsubtype
        ,mob_b_att_active
        ,mob_b_dim_date
        ,mob_b_att_maxstart
        ,IF(mob_s_att_duplicates = 2 AND mob_b_att_active = 1,mob_b_mes_numrgus - 1,mob_b_mes_numrgus) AS mob_b_mes_numrgus
        ,mob_b_fla_tenure
        ,mob_e_att_active
        ,mob_e_dim_date
        ,mob_e_att_maxstart
        ,IF(mob_s_att_duplicates = 2 AND mob_e_att_active = 1,mob_e_mes_numrgus - 1,mob_e_mes_numrgus) AS mob_e_mes_numrgus
        ,mob_e_fla_tenure
        ,mob_s_fla_mainmovement
        ,mob_s_fla_spinmovement
        ,mob_s_fla_churnflag
        ,mob_s_fla_churntype
        ,mob_s_fla_rejoiner
        ,mob_s_att_duplicates
        ,fmc_s_fla_churnflag
        ,fmc_s_fla_churntype
        ,fmc_b_fla_tenure
        ,fmc_e_fla_tenure
        ,fmc_b_fla_tech
        ,fmc_e_fla_tech
        ,fmc_b_fla_fmc
        ,fmc_e_fla_fmc
        ,fmc_b_fla_fmcsegment
        ,fmc_e_fla_fmcsegment
        ,fmc_s_fla_rejoiner
        ,fmc_s_fla_waterfall
        ,IF(mob_s_att_duplicates = 2 AND mob_b_att_active = 1,fmc_b_mes_numrgus - 1,fmc_b_mes_numrgus) AS fmc_b_mes_numrgus
        ,IF(mob_s_att_duplicates = 2 AND mob_e_att_active = 1,fmc_e_mes_numrgus - 1,fmc_e_mes_numrgus) AS fmc_e_mes_numrgus
        ,fmc_s_fla_partialtotalchurn
        ,COUNT(DISTINCT mob_s_att_parentaccount) AS mob_s_att_parentaccount
        ,COUNT(DISTINCT mob_b_mes_tenuredays) AS mob_b_mes_tenuredays
        ,COUNT(DISTINCT mob_b_mes_tenuredays) AS mob_b_mes_tenuredays
        ,COUNT(DISTINCT fmc_s_att_account) AS fmc_s_att_account
        ,COUNT(DISTINCT fix_s_att_account) AS fix_s_att_account
        ,COUNT(DISTINCT mob_s_att_account) AS mob_s_att_account
        --,AVG(CAST(ROUND(mob_s_mes_mrcdiff,0) AS INT)) AS mob_s_mes_mrcdiff
        --,SUM(CAST(ROUND(fmc_b_mes_mrc,0) AS INT)) AS fmc_b_mes_mrc
        --,SUM(CAST(ROUND(fmc_e_mes_mrc,0) AS INT)) AS fmc_e_mes_mrc
        --,SUM(CAST(ROUND(mob_b_mes_mrc,0) AS INT)) AS mob_b_mes_mrc
        --,SUM(CAST(ROUND(mob_e_mes_mrc,0) AS INT)) AS mob_e_mes_mrc
        0 AS mob_s_mes_mrcdiff
        0 AS fmc_b_mes_mrc
        0 AS fmc_e_mes_mrc
        0 AS mob_b_mes_mrc
        0 AS mob_e_mes_mrc
        ,COUNT(DISTINCT fix_b_fla_bb) AS fix_b_fla_bb
        ,COUNT(DISTINCT fix_b_fla_tv) AS fix_b_fla_tv
        ,COUNT(DISTINCT fix_b_fla_vo) AS fix_b_fla_vo
        ,COUNT(DISTINCT fix_e_fla_bb) AS fix_e_fla_bb
        ,COUNT(DISTINCT fix_e_fla_tv) AS fix_e_fla_tv
        ,COUNT(DISTINCT fix_e_fla_vo) AS fix_e_fla_vo
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
GROUP BY    1,2,3,4,5,6,7,8,9,10
            ,11,12,13,14,15,16,17,18,19,20
            ,21,22,23,24,25,26,27,28,29,30
            ,31,32,33,34,37,38,39,40
            ,41,42,43,44,45,46,47,48,49,50
            ,51,52,53,54,55,56,57,58,59,60
            ,61,62,63,64,65,66,67,68,69,70
            ,71,72,73,74
