
/* --> NET ADDS CHECK DE FIJO
SELECT  DISTINCT month
        ,fixedchurntype
        ,fixedmainmovement
        ,sum(b_numrgus) as BOM_RGUs_fix
        ,sum(e_numrgus) as EOM_RGUs_fix
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
WHERE month = date(dt)
    AND month = date('2022-12-01')
GROUP BY 1,2,3
ORDER BY 1,2,3
*/

/* --> NET ADDS CHECK DE MOBILE
SELECT  DISTINCT month
        ,mobilechurnertype
        ,mobilemainmovement
        ,sum(b_mobilergus) as BOM_RGUs_mob
        ,sum(e_mobilergus) as EOM_RGUs_mob
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
WHERE month = date(dt)
GROUP BY 1,2,3
ORDER BY 1,2,3
*/

/* --> VER SI DRC ESTA UPDATED
select distinct dt
FROM "lla_cco_int_ext_prod"."cwp_mov_ext_derecognition"
*/

/* --> GENERAL QUALITY CHECKS
SELECT  DISTINCT month
        ,partial_total_churnflag
        ,churntypefinalflag
        ,FixedChurnType
        ,fixedchurnflag
        ,f_activeBOM
        ,f_activeEOM
        ,mobileChurnerType
        ,mobilechurnflag
        ,mobile_activeBOM
        ,mobile_activeEOM
        ,FmcFlagMob
        ,count(distinct finalaccount) as cuentas
        ,sum(b_totalrgus) as BOM_totRGUs
        ,sum(e_totalrgus) as EOM_totRGUs
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
WHERE month = date(dt) AND month = date('2022-12-01')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12
*/

--> BASE STABILITY
SELECT  DISTINCT month
        ,sum(f_activebom) as Fixed_Active_BOM
        ,sum(f_activeeom) as Fixed_Active_EOM
        ,sum(mobile_activebom) as Mobile_Active_BOM
        ,sum(mobile_activeeom) as Mobile_Active_BOM
        ,sum(final_bom_activeflag) as Final_Active_BOM
        ,sum(final_eom_activeflag) as Final_Active_EOM
FROM "lla_cco_int_ana_dev"."cwp_fmc_churn_dev"
WHERE month = date(dt) --AND month = date('2022-12-01')
GROUP BY 1
ORDER BY 1
