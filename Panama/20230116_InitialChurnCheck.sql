-- Agregar las siguientes lineas de codigo al final del query de fijo para realizar la validacion inicial de churn que se compara contra el query de Juan

select  fixedmonth
        ,fixedchurntype
        ,fixedmainmovement
        ,b_techflag
        ,F_ActiveEOM
        ,count(distinct fixedaccount) as cuentas
        ,sum(B_NumRGUs) as BOM_RGUs
        ,sum(E_NumRGUs) as EOM_RGUs
FROM FinalChurnFlag_SO
WHERE fixedmonth = date('2023-01-01')
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
