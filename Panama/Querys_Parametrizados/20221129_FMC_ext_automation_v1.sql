---------------------------------------------------------------------------------------------------
---------------------------- CWP - FMC EXTERNAL TABLE AUTOMATION ----------------------------------
---------------------------------------------------------------------------------------------------

WITH 

parametros AS (
SELECT  DATE('2022-10-01') AS fecha_cierre_fijo,
        DATE('2022-10-03') AS fecha_cierre_movil_pos,
        DATE('2022-10-03') AS fecha_cierre_movil_pre
-- La fecha de cierre de fijo es el primer día del mes siguiente
-- Fechas de cierre de movil, son input de Jhon Martinez:
---- Septiembre: 2022-10-03
---- Octubre: 2022-11-01
---- Noviembre:
---- Diciembre:
)

,dna_fijo AS (
SELECT  act_acct_cd AS cuenta_fijo
        ,TRY_CAST(ARRAY_JOIN(SPLIT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(act_contact_phone_1,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') AS INTEGER) AS tel_1 
        ,TRY_CAST(ARRAY_JOIN(SPLIT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(act_contact_phone_2,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') AS INTEGER) AS tel_2
        ,TRY_CAST(ARRAY_JOIN(SPLIT(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(act_contact_phone_3,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') AS INTEGER) AS tel_3
        ,TRIM(REPLACE(act_acct_id_val,'-','')) AS cedula_fijo
        ,CASE   WHEN pd_bb_accs_media IS NOT null THEN pd_bb_accs_media 
                WHEN pd_bb_accs_media = 'FTTH' OR pd_TV_accs_media = 'FTTH' OR pd_vo_accs_media = 'FTTH' THEN 'FTTH'
                WHEN pd_bb_accs_media = 'HFC' OR pd_TV_accs_media = 'HFC' OR pd_vo_accs_media = 'HFC' THEN 'HFC' 
                WHEN pd_bb_accs_media = 'FWA' OR pd_TV_accs_media = 'FWA' OR pd_vo_accs_media = 'FWA' THEN 'FWA' 
                WHEN pd_bb_accs_media = 'VDSL' OR pd_TV_accs_media = 'VDSL' OR pd_vo_accs_media = 'VDSL' THEN 'VDSL'
                ELSE 'COPPER' END AS tecnologia
FROM "db-analytics-prod"."fixed_cwp"
WHERE TRY_CAST(dt AS DATE) = (SELECT fecha_cierre_fijo FROM parametros)
AND pd_mix_cd NOT IN ('0P')
)

,dna_movil_pospago AS (
SELECT  TRY_CAST(serviceno AS INTEGER) AS serviceno
        ,TRIM(REPLACE(numero_identificacion,'-','')) AS cedula_movil
        ,CASE WHEN biz_unit_mo = 'B2C' THEN '1. B2C' ELSE '2. B2B OR OTHER' END AS biz_unit_mo
FROM "db-analytics-prod"."tbl_postpaid_cwp"
WHERE TRY_CAST(dt AS DATE) = (SELECT fecha_cierre_movil_pos FROM parametros)
        AND account_status IN ('ACTIVE','RESTRICTED','GROSS_ADDS')
)

,dna_movil_prepago AS (
SELECT  accs_mthd_cd
FROM "db-analytics-prod"."prepaid_cwp" 
WHERE TRY_CAST(dt AS DATE) = (SELECT fecha_cierre_movil_pre FROM parametros)
        AND base_stat IN ('ACTIVE BASE', 'GROSS ADD', 'REJOINER')
)

,FMC AS (
SELECT  CAST(household_id AS VARCHAR) AS cuenta_fijo
        ,service_id AS serviceno
        ,'1. Inscrito a Paquete completo' AS tipo_convergente
        ,biz_unit_mo
        ,'Pospago' AS telefonia 
FROM    (SELECT * 
        FROM "db-stage-dev"."cwp_reporte_fmc" 
        WHERE DATE(fecha_cierre) = ((SELECT fecha_cierre_fijo FROM parametros) - interval '1' day)) A
LEFT JOIN dna_fijo B ON (A.household_id = CAST(B.cuenta_fijo AS BIGINT))
LEFT JOIN dna_movil_pospago C ON (A.service_id = C.serviceno)
WHERE B.cuenta_fijo IS NOT null
        AND C.serviceno IS NOT null
)

,match_id AS (
SELECT  cuenta_fijo
        ,serviceno
        ,'2. match_id' AS tipo_convergente
        ,biz_unit_mo
        ,'Pospago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_pospago ON (cedula_fijo = cedula_movil)
)

,contact_number_pospago_1 AS (
SELECT  cuenta_fijo
        ,serviceno
        ,'3. Contact number' AS tipo_convergente
        ,biz_unit_mo
        ,'Pospago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_pospago ON (tel_1 = serviceno)
WHERE serviceno NOT IN (6000000)
)

,contact_number_pospago_2 AS (
SELECT  cuenta_fijo
        ,serviceno
        ,'3. Contact number' AS tipo_convergente
        ,biz_unit_mo
        ,'Pospago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_pospago ON (tel_2 = serviceno)
WHERE serviceno NOT IN (6000000)
)

,contact_number_pospago_3 AS (
SELECT  cuenta_fijo
        ,serviceno
        ,'3. Contact number' AS tipo_convergente
        ,biz_unit_mo
        ,'Pospago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_pospago ON (tel_3 = serviceno)
WHERE serviceno NOT IN (6000000)
)

,contact_number_prepago_1 AS (
SELECT  cuenta_fijo
        ,accs_mthd_cd
        ,'3. Contact number' AS tipo_convergente
        ,'1. B2C' AS biz_unit_mo
        ,'Prepago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_prepago ON (tel_1 = accs_mthd_cd)
WHERE accs_mthd_cd NOT IN (6000000)
)

,contact_number_prepago_2 AS (
SELECT  cuenta_fijo
        ,accs_mthd_cd
        ,'3. Contact number' AS tipo_convergente
        ,'1. B2C' AS biz_unit_mo
        ,'Prepago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_prepago ON (tel_2 = accs_mthd_cd)
WHERE accs_mthd_cd NOT IN (6000000)
)

,contact_number_prepago_3 AS (
SELECT  cuenta_fijo
        ,accs_mthd_cd
        ,'3. Contact number' AS tipo_convergente
        ,'1. B2C' AS biz_unit_mo
        ,'Prepago' AS telefonia
FROM dna_fijo 
JOIN dna_movil_prepago ON (tel_3 = accs_mthd_cd)
WHERE accs_mthd_cd NOT IN (6000000)
)

,beneficio_manual AS (
SELECT *
FROM "db-stage-dev"."cwp_beneficio_convergente"
) 

,base_convergente AS (
SELECT * FROM FMC
UNION ALL 
SELECT * FROM match_id
UNION ALL 
SELECT * FROM contact_number_pospago_1
UNION ALL 
SELECT * FROM contact_number_pospago_2
UNION ALL 
SELECT * FROM contact_number_pospago_3
UNION ALL 
SELECT * FROM contact_number_prepago_1
UNION ALL 
SELECT * FROM contact_number_prepago_2
UNION ALL 
SELECT * FROM contact_number_prepago_3
)

,base_convergente_fijo AS (
SELECT  *,row_number() OVER (PARTITION BY cuenta_fijo ORDER BY telefonia, biz_unit_mo, tipo_convergente) AS rnk
FROM base_convergente
)

,base_convergente_movil AS (
SELECT *,row_number() OVER (PARTITION BY serviceno ORDER BY telefonia, biz_unit_mo, tipo_convergente) AS rnk
FROM base_convergente
)

,tabla_final AS (
SELECT  A.cuenta_fijo
        ,serviceno
        ,CASE   WHEN biz_unit_mo IN ('2. B2B OR OTHER') THEN '0. B2C/SMB'
                WHEN telefonia = 'Prepago' THEN '4. Convergente Prepago'
                ELSE tipo_convergente END AS tipo_convergente
        ,biz_unit_mo
        ,telefonia
        ,tecnologia
FROM base_convergente_fijo A
LEFT JOIN dna_fijo B ON (A.cuenta_fijo = B.cuenta_fijo)
WHERE rnk = 1  
)

--,beneficio_cruce AS (
SELECT  CAST(cuenta_fijo AS BIGINT) AS household_id
        ,CAST(serviceno AS BIGINT) AS service_id
        ,CASE   WHEN CAST(cuenta_fijo AS BIGINT) IN (SELECT DISTINCT cuenta FROM beneficio_manual) 
                    AND telefonia = 'Pospago' 
                    AND biz_unit_mo = '1. B2C' 
                    AND tipo_convergente <> '1. Inscrito a Paquete completo' 
                    THEN '2. Beneficio manual' 
                ELSE tipo_convergente END AS tipo
        ,telefonia
        ,REPLACE(CAST(DATE_TRUNC('month', (SELECT fecha_cierre_fijo FROM parametros) - INTERVAL '10' DAY) AS VARCHAR),'-') AS "date"
        ,biz_unit_mo AS "unidad de negocio"
FROM tabla_final
--)

/*
SELECT "date", tipo, telefonia,"unidad de negocio", count(distinct household_id), count(distinct service_id)
FROM beneficio_cruce
group by 1,2,3,4
order by 1,2,3,4
*/
