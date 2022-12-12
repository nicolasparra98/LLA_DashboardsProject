--CREATE TABLE IF NOT EXISTS "lla_cco_int_stg"."cwp_sp3_BASEKPIs_dashboardinput_dinamico_RJ_v3" AS

WITH 

FixedConvergency AS(
 SELECT * 
 FROM "lla_cco_int_stg"."cwp_fix_stg_dashboardinput_dinamico_RJ_v3_mar" 
)

,MobileConvergency AS(
SELECT * 
FROM "lla_cco_int_stg"."cwp_mob_stg_dashboardinput_dinamico_RJ_v2_mar" 
)

,FullCustomerBase AS(
SELECT DISTINCT
CASE WHEN CAST(f.fixedaccount AS VARCHAR(50))=CAST(m.Mobile_household_id AS VARCHAR(50)) THEN f.fixedmonth
      WHEN f.fixedaccount IS NOT NULL AND m.Mobile_household_id IS NULL THEN f.fixedmonth
      WHEN f.fixedaccount IS NULL AND m.Mobile_household_id IS NOT NULL THEN m.Mobile_Month
      WHEN f.fixedaccount IS NULL AND f.FixedAccount IS NOT NULL THEN f.fixedmonth
      WHEN m.Mobile_household_id IS NULL AND m.Mobile_Account IS NOT NULL THEN m.Mobile_Month
      END AS Month
,CASE WHEN CAST(f.fixedaccount AS VARCHAR(50))=CAST(m.Mobile_household_id AS VARCHAR(50)) THEN CAST(f.FixedAccount AS VARCHAR(50))
      WHEN f.fixedaccount IS NOT NULL AND m.Mobile_household_id IS NULL THEN CAST(f.FixedAccount AS VARCHAR(50))
      WHEN f.fixedaccount IS NULL AND m.Mobile_household_id IS NOT NULL THEN CAST(m.Mobile_Account as VARCHAR(50))
      WHEN f.fixedaccount IS NULL AND f.FixedAccount IS NOT NULL THEN CAST(f.FixedAccount as VARCHAR(50))
      WHEN m.Mobile_household_id IS NULL AND m.Mobile_Account IS NOT NULL THEN CAST(m.Mobile_Account as VARCHAR(50))
      END AS FinalAccount,
     -- FixedAccount, Mobile_Account
CASE WHEN (F_ActiveBOM =1 AND Mobile_ActiveBOM=1) or (F_ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((F_ActiveBOM=0 OR F_ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag,
CASE WHEN (F_ActiveEOM =1 AND Mobile_ActiveEOM=1) or (F_ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((F_ActiveEOM=0 OR F_ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag
,CASE WHEN f.FmcFlagFix=m.FmcFlagMob THEN f.FmcFlagFix
      WHEN (f.FmcFlagFix='4.Fixed Only' AND m.FmcFlagMob IS NULL) OR f.fmcFlagFix= '3. Hard Bundle' THEN f.fmcFlagFix
      WHEN fmcFlagFix IS NOT NULL and FmcFlagMob IS NULL THEN '4.Fixed Only'
      WHEN FmcFlagMob IS NOT NULL AND fmcFlagFix IS NULL THEN '4.Mobile Only'
      END AS FmcFlag
,f.*, m.*
,CASE WHEN (FixedChurnFlag = '1. Fixed Churner' AND MobileChurnFlag = '1. Mobile Churner') THEN 'Churner'
      WHEN (FixedChurnFlag = '1. Fixed Churner' AND (MobileChurnFlag =  '2. Mobile NonChurner' OR MobileChurnFlag IS NULL) ) THEN 'Fixed Churner'
      WHEN ((FixedChurnFlag = '2. Fixed NonChurner' OR FixedChurnFlag IS NULL) AND MobileChurnFlag =  '1. Mobile Churner') THEN 'Mobile Churner'
      ELSE 'Non Churner' END AS FinalChurnFlag
FROM FixedConvergency f FULL OUTER JOIN MobileConvergency m ON CAST(f.fixedaccount AS VARCHAR(50))=CAST(m.Mobile_household_id AS VARCHAR(50)) and f.fixedmonth = m.Mobile_Month
)
,ChurnFinalFlags AS(
SELECT DISTINCT f.*
,CASE WHEN (FinalChurnFlag = 'Churner' AND FixedChurnType = '1. Fixed Voluntary Churner' AND MobileChurnerType = '1. Mobile Voluntary Churner')
 OR (FinalChurnFlag = 'Fixed Churner' AND FixedChurnType = '1. Fixed Voluntary Churner') OR (FinalChurnFlag = 'Mobile Churner' AND MobileChurnerType = '1. Mobile Voluntary Churner') THEN 'Voluntary'
WHEN (FinalChurnFlag = 'Churner' AND FixedChurnType = '2. Fixed Involuntary Churner' AND MobileChurnerType = '2. Mobile Involuntary Churner')
 OR (FinalChurnFlag = 'Fixed Churner' AND FixedChurnType = '2. Fixed Involuntary Churner') OR (FinalChurnFlag = 'Mobile Churner' AND MobileChurnerType = '2. Mobile Involuntary Churner') THEN 'Involuntary'
WHEN FinalChurnFlag = 'Churner' AND ((FixedChurnType = '2. Fixed Involuntary Churner' AND MobileChurnerType = '1. Mobile Voluntary Churner')
 OR (FixedChurnType = '1. Fixed Voluntary Churner' AND MobileChurnerType = '2. Mobile Involuntary Churner')) THEN 'Mixed'
WHEN FixedChurnType='3. Fixed 0P Churner' and  (MobileChurnFlag =  '2. Mobile NonChurner' or MobileChurnFlag is null) then '0P Churner' 
END AS ChurnTypeFinalFlag,
(coalesce(B_NumRGUs,0) + coalesce(B_MobileRGUs,0)) as B_TotalRGUs
,(coalesce(E_NumRGUs,0) + coalesce(E_MobileRGUs,0)) AS E_TotalRGUs
FROM FullCustomerBase f
)
,FullCustomerBase_Flags AS(
  SELECT *,
  CASE WHEN (B_FixedTenure IS NOT NULL AND B_MobileTenure IS NULL) THEN B_FixedTenure
  WHEN (B_FixedTenure = B_MobileTenure) THEN B_FixedTenure
  WHEN (B_MobileTenure IS NOT NULL AND B_FixedTenure IS NULL) THEN B_MobileTenure
  WHEN (B_FixedTenure <> B_MobileTenure AND (B_FixedTenure = 'Early-Tenure'  or B_MobileTenure = 'Early-Tenure' )) THEN 'Early-Tenure'
  END AS B_Final_Tenure,
  CASE WHEN (E_FixedTenure IS NOT NULL AND E_MobileTenure IS NULL) THEN E_FixedTenure
  WHEN (E_FixedTenure = E_MobileTenure) THEN E_FixedTenure
  WHEN (E_MobileTenure IS NOT NULL AND E_FixedTenure IS NULL) THEN E_MobileTenure
  WHEN (E_FixedTenure <> E_MobileTenure AND (E_FixedTenure = 'Early-Tenure'  or E_MobileTenure = 'Early-Tenure' )) THEN 'Early-Tenure'
  END AS E_Final_Tenure,
  CASE WHEN Fixed_PRMonth=1 AND Mobile_PRMonth=1 THEN '1.Full Potential Rejoiner'
  WHEN Fixed_PRMonth=1 AND(Mobile_PRMonth=0 OR Mobile_PRMonth IS NULL) THEN '2. Fixed Potential Rejoiner'
  WHEN Mobile_PRMonth=1 AND(Fixed_PRMonth=0 OR Fixed_PRMonth IS NULL) THEN '3. Mobile Potential Rejoiner'
  ELSE NULL END AS PotentialRejoinerFlag
,CASE WHEN Fixed_RejoinerMonth=1 AND Mobile_RejoinerMonth=1 THEN '1.Full Rejoiner'
  WHEN Fixed_RejoinerMonth=1 AND(Mobile_RejoinerMonth=0 OR Mobile_RejoinerMonth IS NULL) THEN '2. Fixed Rejoiner'
  WHEN Mobile_RejoinerMonth=1 AND(Fixed_RejoinerMonth=0 OR Fixed_RejoinerMonth IS NULL) THEN '3. Mobile Rejoiner'
  ELSE NULL END AS RejoinerFlag
FROM ChurnFinalFlags
)
,FmcTypeFlags AS(
SELECT *
,CASE WHEN F_ActiveBOM=0 AND Mobile_ActiveBOM=0 THEN NULL
  WHEN (FMCFlag = '4.Mobile Only' and Mobile_ActiveBOM=1) OR ((FMCFlag='1.Soft FMC' OR FMCFlag='2.Near FMC') AND ((F_ActiveBOM=0 OR F_ActiveBOM IS NULL) AND Mobile_ActiveBOM=1)) THEN 'MobileOnly'
  WHEN B_MixCode_Adj = '1P' AND((Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL) AND F_ActiveBOM=1) THEN 'Fixed 1P'
  WHEN B_MixCode_Adj = '2P' AND((Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL) AND F_ActiveBOM=1) THEN 'Fixed 2P'
  WHEN B_MixCode_Adj = '3P' AND((Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL) AND F_ActiveBOM=1) THEN 'Fixed 3P'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle') THEN 'Real FMC'
  WHEN FMCFlag = '2.Near FMC' THEN 'Near FMC'
  END AS B_FMCType
,CASE WHEN F_ActiveEOM=1 AND (FinalChurnFlag='Churner' or FinalChurnFlag='Fixed Churner') THEN 'ChurnerActiveEOM'
  WHEN F_ActiveEOM=0 AND Mobile_ActiveEOM=0 THEN NULL
  WHEN (FMCFlag = '4.Fixed Only' OR ((FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle') OR FMCFlag =  '2.Near FMC') 
   AND (F_ActiveEOM = 0 OR F_ActiveEOM IS NULL))AND E_MixCode_Adj IS NULL AND FixedChurnFlag <> '1. Fixed Churner'  THEN 'Fixed Gap Customer'
  WHEN (FMCFlag = '4.Mobile Only' and Mobile_ActiveEOM=1) OR ((FMCFlag='1.Soft FMC' OR FMCFlag='2.Near FMC' OR FMCFlag = '3. Hard Bundle') AND ((F_ActiveEOM=0 OR F_ActiveEOM IS NULL) AND Mobile_ActiveEOM=1))
   AND (FinalChurnFlag<>'Mobile Churner' AND FinalChurnFlag<>'Churner' )
    or ((FMCFlag = '1.Soft FMC' or FMCFlag = '2.Near FMC' OR FMCFlag = '3.Hard Bundle') and (F_ActiveEOM=1 AND fixedchurnflag = '1. Fixed Churner')) 
   THEN 'MobileOnly'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle') AND FixedChurnFlag = '2. Fixed NonChurner' and MobileChurnFlag != '1. Mobile Churner' THEN 'Real FMC'
  WHEN FMCFlag = '2.Near FMC' AND FixedChurnFlag = '2. Fixed NonChurner' and MobileChurnFlag != '1. Mobile Churner' THEN 'Near FMC'
  WHEN (FMCFlag = '4.Fixed Only' and E_MixCode_Adj = '1P' AND FinalChurnFlag<>'Fixed Churner' AND FinalChurnFlag<>'Churner' 
   AND((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1))
   or ((FMCFlag = '1.Soft FMC' or FMCFlag = '2.Near FMC' OR FMCFlag = '3.Hard Bundle') and ((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1 AND E_MixCode_Adj = '1P'))
   THEN 'Fixed 1P'
  WHEN FMCFlag = '4.Fixed Only' and E_MixCode_Adj = '2P' AND FinalChurnFlag<>'Fixed Churner' AND FinalChurnFlag<>'Churner'
   AND((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1) 
   OR ((FMCFlag = '1.Soft FMC' or FMCFlag = '2.Near FMC' OR FMCFlag = '3.Hard Bundle') and ((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1 AND E_MixCode_Adj = '2P')) 
   THEN 'Fixed 2P'
  WHEN FMCFlag = '4.Fixed Only' and E_MixCode_Adj = '3P' AND FinalChurnFlag<>'Fixed Churner' AND FinalChurnFlag<>'Churner'
   AND((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1) 
   OR ((FMCFlag = '1.Soft FMC' or FMCFlag = '2.Near FMC' OR FMCFlag = '3.Hard Bundle') and ((Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL) AND F_ActiveEOM=1 AND E_MixCode_Adj = '3P'))
   THEN 'Fixed 3P'
  END AS E_FMCType
FROM FullCustomerBase_Flags
)
,FmcSegmentFlags AS(
SELECT *
,CASE WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = '1P' THEN 'P2'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = '2P' THEN 'P3'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveBOM = 1 AND Mobile_ActiveBOM = 1) AND  B_MixCode_Adj = '3P' THEN 'P4'
  WHEN (F_ActiveBOM=1 AND (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)) THEN 'P1 Fixed'
  WHEN (F_ActiveBOM= 0 OR F_ActiveBOM IS NULL) AND Mobile_ActiveBOM= 1 THEN 'P1 Mobile'
  END AS B_FMCSegment
,CASE WHEN (E_FMCType = 'Fixed Gap Customer') then E_FMCType
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj ='1P' AND FixedChurnFlag = '2. Fixed NonChurner' THEN 'P2'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj = '2P' AND FixedChurnFlag = '2. Fixed NonChurner' THEN 'P3'
  WHEN (FMCFlag = '1.Soft FMC' OR FMCFlag = '3. Hard Bundle' or FMCFlag = '2.Near FMC' ) AND (F_ActiveEOM = 1 AND Mobile_ActiveEOM = 1) AND  E_MixCode_Adj = '3P' AND FixedChurnFlag = '2. Fixed NonChurner' THEN 'P4'
  WHEN ((F_ActiveEOM=1 AND (Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL))) AND FixedChurnFlag = '2. Fixed NonChurner' THEN 'P1 Fixed'
  WHEN ((F_ActiveEOM= 0 OR F_ActiveEOM IS NULL OR FixedChurnFlag = '1. Fixed Churner') AND Mobile_ActiveEOM= 1)
   AND (FinalChurnFlag<>'Mobile Churner' AND FinalChurnFlag<>'Churner' ) THEN 'P1 Mobile'
  END AS E_FMCSegment
,ROUND((CASE WHEN B_Fixed_MRC IS NULL THEN 0 ELSE B_Fixed_MRC END + CASE WHEN B_MobileMRC IS NULL THEN 0 ELSE B_MobileMRC END),0) AS B_Total_MRC
,ROUND((CASE WHEN E_Fixed_MRC IS NULL THEN 0 ELSE E_Fixed_MRC END + CASE WHEN E_MobileMRC IS NULL THEN 0 ELSE E_MobileMRC END),0) AS E_Total_MRC
FROM FmcTypeFlags
)
,
FullCustomerBaseFlags_Waterfall AS(
SELECT f.*
,CASE WHEN (FinalChurnFlag = 'Churner') OR (FinalChurnFlag = 'Fixed Churner' and B_FMCSegment = 'P1 Fixed' and E_FMCSegment is null) OR (FinalChurnFlag = 'Mobile Churner' and B_FMCSegment = 'P1 Mobile' and E_FMCSegment is null) then 'Total Churner'
WHEN FinalChurnFlag = 'Non Churner' then null
ELSE 'Partial Churner' end as Partial_Total_ChurnFlag
,CASE WHEN (RejoinerFlag='2. Fixed Rejoiner' OR RejoinerFlag='3. Mobile Rejoiner') AND E_FMCSegment IN('P2','P3','P4') THEN '1. FMC Rejoiner'
 ELSE RejoinerFlag END AS RejoinerFMCFlag
,CASE 
 WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND ((FixedMainMovement = '4.New Customer' AND MobileMainMovement = '4.New Customer')
  OR (FixedMainMovement = '4.New Customer' AND MobileMainMovement IS NULL) OR (FixedMainMovement IS NULL AND MobileMainMovement = '4.New Customer'))  THEN 'Gross Adds'
 WHEN (F_ActiveBOM = 0 and F_ActiveEOM = 1) AND FixedMainMovement IS NULL AND Fixed_E_MaxStart IS NULL THEN 'GrossAdd-Fixed Customer Gap'
  WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (FixedMainMovement = '5.Come Back to Life' OR MobileMainMovement = '5.Come Back to Life')
   AND FinalChurnFlag <> 'Non Churner' THEN 'ComeBackToLife-Fixed Customer Gap'
 WHEN Final_BOM_ActiveFlag = 0 AND Final_EOM_ActiveFlag = 1 AND (RejoinerFlag='2. Fixed Rejoiner' OR RejoinerFlag='3. Mobile Rejoiner') AND E_FMCSegment IN('P2','P3','P4') THEN '5.1. Real FMC Rejoiner'
 WHEN Final_BOM_ActiveFlag = 0  AND Final_EOM_ActiveFlag = 1 AND RejoinerFlag='2. Fixed Rejoiner' AND E_FMCSegment NOT IN('P2','P3','P4') THEN '5.2. Real Fixed Rejoiner'
 WHEN Final_BOM_ActiveFlag = 0 AND Final_EOM_ActiveFlag = 1 AND RejoinerFlag='3. Mobile Rejoiner' AND E_FMCSegment NOT IN('P2','P3','P4') THEN '5.3. Real Mobile Rejoiner'
 WHEN Final_BOM_ActiveFlag = 1 AND Final_EOM_ActiveFlag = 1 AND RejoinerFlag IS NOT NULL THEN '5.4 Near Rejoiner'
 WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND (FixedMainMovement = '5.Come Back to Life' OR MobileMainMovement = '5.Come Back to Life') AND RejoinerFlag IS NULL THEN 'Gross Adds'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs < E_TotalRGUs) THEN 'Upsell'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs > E_TotalRGUs) THEN 'Downsell'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_Total_MRC = E_Total_MRC) THEN 'Maintain'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_Total_MRC < E_Total_MRC) THEN 'Upspin'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_TotalRGUs = E_TotalRGUs) AND (B_Total_MRC > E_Total_MRC) THEN 'Downspin'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Voluntary') THEN 'Voluntary Churners'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Involuntary') THEN 'Involuntary Churners'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND (FinalChurnFlag <> 'Non Churner' AND ChurnTypeFinalFlag = 'Mixed') THEN 'Mixed Churners'
 WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 0) AND FixedMainMovement='6.Null last day' AND FinalChurnFlag = 'Non Churner' THEN 'Loss-Fixed Customer Gap'
END AS Waterfall_Flag,
CASE WHEN B_FMCType = 'MobileOnly' THEN 'Wireless'
WHEN  B_FMCType ='Fixed 1P' or B_FMCType ='Fixed 2P' or B_FMCType ='Fixed 3P' or B_FMCType = 'Near FMC' OR B_FMCType = 'Real FMC' THEN B_TechFlag
END AS B_Final_TechFlag,
CASE WHEN E_FMCType = 'MobileOnly' THEN 'Wireless'
WHEN  E_FMCType ='Fixed 1P' or E_FMCType ='Fixed 2P' or E_FMCType ='Fixed 3P' or E_FMCType = 'Near FMC' OR E_FMCType = 'Real FMC' THEN E_TechFlag
END AS E_Final_TechFlag
FROM FmcSegmentFlags f
)
,Final_Flags as(
select distinct Month,FinalAccount,Final_BOM_ActiveFlag,Final_EOM_ActiveFlag,FmcFlag,fixedmonth,fixedaccount,f_activebom,f_activeeom,fix_b_date,fixedaccount_bom,fixed_b_phone,b_overdue,fixed_b_maxstart,b_fixedtenure,b_fixed_mrc,b_techflag,b_numrgus,b_mixname_adj,b_mixcode_adj,b_bb,b_tv,b_vo,b_bbcode,b_tvcode,b_vocode,b_hard_fmc_flag,fix_e_date,fixed_e_phone,e_overdue,fixed_e_maxstart,e_fixedtenure,e_fixed_mrc,e_techflag,e_numrgus,e_mixname_adj,e_mixcode_adj,e_bb,e_tv,e_vo,e_bbcode,e_tvcode,e_vocode,e_hard_fmc_flag,first_sales_chnl_bom,last_sales_chnl_bom,first_sales_chnl_eom,last_sales_chnl_eom,fixedmainmovement,fixedspinmovement,fixedchurnflag,fixedchurntype,fixedchurnsubtype,household_id,fmcflagfix,fixed_prmonth,fixed_rejoinermonth,gap,mobile_month,mobile_account,phonenumber,mobile_activebom,mobile_activeeom,b_date,phone_bom,mobile_b_maxstart,b_mob_acc_name,b_mobile_id,b_mobilemrc,b_mobilergus,b_avgmobilemrc,b_mobiletenure,e_date,phone_eom,mobile_e_maxstart,e_mob_acc_name,e_mobile_id,e_mobilemrc,e_mobilergus,e_avgmobilemrc,e_mobiletenure,mobile_mrc_diff,mobilemainmovement,mobilespinflag
,case when mobile_household_id is not null and e_fmcsegment = 'P1 Mobile' then null
	 when mobile_household_id is not null and (e_fmcsegment <> 'P1 Mobile' or e_fmcsegment is null) then mobile_household_id
 end as mobile_household_id
,case when mobile_household_id is not null and e_fmcsegment = 'P1 Mobile' then null
	when mobile_household_id is not null and (e_fmcsegment <> 'P1 Mobile' or e_fmcsegment is null) then fmcflagmob
	end as fmcflagmob
,drc,mobilechurnflag,mobilechurnertype,mobile_prmonth,mobile_rejoinermonth,FinalChurnFlag,ChurnTypeFinalFlag,B_TotalRGUs,E_TotalRGUs,B_Final_Tenure,E_Final_Tenure,PotentialRejoinerFlag,RejoinerFlag,B_FMCType,E_FMCType,B_FMCSegment,E_FMCSegment,B_Total_MRC,E_Total_MRC,Partial_Total_ChurnFlag,RejoinerFMCFlag,Waterfall_Flag,B_Final_TechFlag,E_Final_TechFlag
,Case when waterfall_flag='Downsell' and FixedMainMovement='3.Downsell' then 'Voluntary'
      when waterfall_flag='Downsell' and FinalChurnFlag <> 'Non Churner' then ChurnTypeFinalFlag
      when waterfall_flag='Downsell' and fixedmainmovement='6.Null last day' then 'Undefined'
else null end as Downsell_Split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as Downspin_Split
from FullCustomerBaseFlags_Waterfall f
)

SELECT *
from Final_flags
where month=date('2022-02-01')
