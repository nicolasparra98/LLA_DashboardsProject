WITH 

-- Enero:
-- Febrero:
-- Marzo:
-- Abril:
-- Mayo:
-- Junio:
-- Julio:
-- Agosto:
-- Septiembre: 2022-10-03
-- Octubre: 2022-11-01
-- Noviembre:
-- Diciembre:


parametros as (
select
--Aqui se cambian las fechas
date('2022-10-01') as Fecha_cierre_fijo,
date('2022-10-03') as Fecha_cierre_movil_pos,
date('2022-10-03') as Fecha_cierre_movil_pre
)
,
DNA_fijo as(
SELECT a.act_acct_cd Cuenta_fijo,
try_cast(array_join(split(TRIM(replace(replace(replace(replace(act_contact_phone_1,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') as integer) Tel_1, 
try_cast(array_join(split(TRIM(replace(replace(replace(replace(act_contact_phone_2,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') as integer) Tel_2,
try_cast(array_join(split(TRIM(replace(replace(replace(replace(act_contact_phone_3,'Ext. 0000',''),'EXT. 0000',''),'-',''),'/','')),',',8),'') as integer) Tel_3,
TRIM(replace(act_acct_id_val,'-','')) Cedula_Fijo,
CASE WHEN pd_bb_accs_media is not null THEN pd_bb_accs_media WHEN pd_bb_accs_media = 'FTTH' OR pd_TV_accs_media = 'FTTH' OR pd_vo_accs_media = 'FTTH' THEN 'FTTH' WHEN pd_bb_accs_media = 'HFC' OR pd_TV_accs_media = 'HFC' OR pd_vo_accs_media = 'HFC' THEN 'HFC' WHEN pd_bb_accs_media = 'FWA' OR pd_TV_accs_media = 'FWA' OR pd_vo_accs_media = 'FWA' THEN 'FWA' WHEN pd_bb_accs_media = 'VDSL' OR pd_TV_accs_media = 'VDSL' OR pd_vo_accs_media = 'VDSL' THEN 'VDSL' ELSE 'COPPER'END AS Tecnologia
FROM "db-analytics-prod"."fixed_cwp" a
-- LEFT JOIN (select * from "cwp-marketing"."drc_fixed" WHERE fecha_drc='2022-09-30') b on (CAST(a.act_acct_cd AS BIGINT)=b.act_acct_cd)
WHERE TRY_CAST (dt as date)= (select Fecha_cierre_fijo from parametros)
AND pd_mix_cd not in ('0P')
-- AND b.act_acct_cd is null
)
,
DNA_Movil_Pospago as (
SELECT try_cast(serviceno as integer) serviceno, TRIM(replace(numero_identificacion,'-',''))  as Cedula_Movil, CASE WHEN biz_unit_mo='B2C' THEN '1. B2C' ELSE '2. B2B OR OTHER' END biz_unit_mo
FROM "db-analytics-prod"."tbl_postpaid_cwp" a
-- left join (SELECT * FROM "cwp-marketing"."drc_movil" WHERE fecha_drc='2022-09-30') b on (CAST(a.serviceno AS BIGINT) =b.act_acct_cd)
WHERE try_cast(DT as date) = (select Fecha_cierre_movil_pos from parametros)
and account_status in ('ACTIVE','RESTRICTED','GROSS_ADDS')
-- and b.act_acct_cd is null
)
-- select * from DNA_Movil_Pospago where serviceno = 63786149
,
DNA_Movil_Prepago as (
SELECT accs_mthd_cd
FROM "db-analytics-prod"."prepaid_cwp" 
WHERE try_cast(dt as date)=  (select Fecha_cierre_movil_pre from parametros)
and base_stat in ('ACTIVE BASE', 'GROSS ADD', 'REJOINER')
)
,
FMC AS (
select cast (household_id as VARCHAR) AS Cuenta_fijo , service_id as serviceno, '1. Inscrito a Paquete completo' AS Tipo_Convergente, biz_unit_mo,  'Pospago' Telefonia 
from (select * from "db-stage-dev"."cwp_reporte_fmc" where date(fecha_cierre) = ((select Fecha_cierre_fijo from parametros) - interval '1' day)) a
left join DNA_fijo b ON (A.household_id=cast (B.Cuenta_fijo as BIGINT))
left join DNA_Movil_Pospago c on (a.service_id=C.serviceno)
WHERE B.Cuenta_fijo is not null
AND C.serviceno is not null
)
,
Match_ID AS (
Select Cuenta_fijo, serviceno, '2. Match_ID' Tipo_Convergente, biz_unit_mo, 'Pospago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Pospago ON (Cedula_Fijo=Cedula_Movil)
)
-- select * from Match_ID where cedula_movil = '88771408'
,
Contact_number_Pospago_1 AS (
Select Cuenta_fijo, serviceno, '3. Contact number' Tipo_Convergente, biz_unit_mo, 'Pospago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Pospago ON (Tel_1=serviceno )
WHERE serviceno <>6000000
)
,
Contact_number_Pospago_2 AS (
Select Cuenta_fijo, serviceno, '3. Contact number' Tipo_Convergente, biz_unit_mo, 'Pospago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Pospago ON (Tel_2=serviceno )
WHERE serviceno not in (6000000)
)
,
Contact_number_Pospago_3 AS (
Select Cuenta_fijo, serviceno, '3. Contact number' Tipo_Convergente, biz_unit_mo, 'Pospago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Pospago ON (Tel_3=serviceno )
WHERE serviceno not in (6000000)
)
,
Contact_number_Prepago_1 AS (
Select Cuenta_fijo, accs_mthd_cd, '3. Contact number' Tipo_Convergente, '1. B2C' biz_unit_mo, 'Prepago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Prepago ON (Tel_1=accs_mthd_cd )
WHERE accs_mthd_cd not in (6000000)
)
,
Contact_number_Prepago_2 AS (
Select Cuenta_fijo, accs_mthd_cd, '3. Contact number' Tipo_Convergente, '1. B2C' biz_unit_mo, 'Prepago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Prepago ON (Tel_2=accs_mthd_cd )
WHERE accs_mthd_cd not in (6000000)
)
,
Contact_number_Prepago_3 AS (
Select Cuenta_fijo, accs_mthd_cd, '3. Contact number' Tipo_Convergente, '1. B2C' biz_unit_mo, 'Prepago' Telefonia
from DNA_fijo 
JOIN DNA_Movil_Prepago ON (Tel_3=accs_mthd_cd)
WHERE accs_mthd_cd not in (6000000)
)
,
beneficio_manual as (SELECT *--cuenta as Cuenta_fijo, '11111111' serviceno, '2. Beneficio manual' Tipo_Convergente, biz_unit_mo, 'Pospago' Telefonia 
FROM "db-stage-dev"."cwp_beneficio_convergente") 

,Base_convergente as(
SELECT * 
FROM FMC
UNION ALL 
SELECT * 
FROM Match_ID
UNION ALL 
Select * from Contact_number_Pospago_1
UNION ALL 
Select * from Contact_number_Pospago_2
UNION ALL 
Select * from Contact_number_Pospago_3
UNION ALL 
Select * from Contact_number_Prepago_1
UNION ALL 
Select * from Contact_number_Prepago_2
UNION ALL 
Select * from Contact_number_Prepago_3
)
,
Base_convergente_fijo as(
Select *, row_number() OVER (PARTITION BY Cuenta_Fijo ORDER BY Telefonia, biz_unit_mo, Tipo_Convergente) AS rnk
From Base_convergente
--limit 10
)
,
Base_convergente_movil as(
Select *, row_number() OVER (PARTITION BY serviceno ORDER BY Telefonia, biz_unit_mo, Tipo_Convergente) AS rnk
From Base_convergente
--limit 10
)

,tabla_final as (
select a.Cuenta_fijo, serviceno, CASE WHEN biz_unit_mo in ('2. B2B OR OTHER') THEN '0. B2C/SMB' WHEN Telefonia='Prepago' THEN '4. Convergente Prepago' ELSE Tipo_Convergente END AS Tipo_Convergente, biz_unit_mo, Telefonia, Tecnologia
from Base_convergente_fijo A
LEFT JOIN DNA_Fijo B ON (A.Cuenta_fijo=B.Cuenta_Fijo)
where rnk=1  
)

--,Beneficio_cruce as (
select  cast(Cuenta_fijo as bigint) as household_id,
        cast(serviceno as bigint) as service_id,
        case when cast(cuenta_fijo as bigint) in (select distinct cuenta from beneficio_manual) and telefonia = 'Pospago' and biz_unit_mo= '1. B2C' and tipo_convergente <> '1. Inscrito en paquete completo' then '2. Beneficio manual' else tipo_convergente end as tipo,
        telefonia,
        replace(cast(date_trunc('month', (select Fecha_cierre_fijo from parametros) - interval '10' day) as varchar),'-') as "date",
        biz_unit_mo as "unidad de negocio"
from tabla_final
where cuenta_fijo is not null
--)

--select distinct "unidad de negocio" from beneficio_cruce

/* 
select "date", tipo, telefonia,"unidad de negocio", count(distinct household_id), count(distinct service_id)
from beneficio_cruce
group by 1,2,3,4
order by 1,2,3,4
*/
