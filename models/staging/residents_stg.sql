
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131










-- Prior to staging, SQL queries were employed in Postgres to determine if ranges were appropriate to meet the criteria.
---------------------------------------------------------------------------------------------------------------------------#

-- Example:

-- SELECT
--    MAX(Total_Population_Male) AS max_Total_Population_Male,
--    MIN(Total_Population_Male) AS min_Total_Population_Male,
--    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Total_Population_Male) AS median_Total_Population_Male,
--    AVG(Total_Population_Male) AS mean_Total_Population_Male,
--    (MAX(Total_Population_Male) - MIN(Total_Population_Male)) AS range_Total_Population_Male,
    
--    MAX(Total_Population_Female) AS max_Total_Population_Female,
--    MIN(Total_Population_Female) AS min_Total_Population_Female,
--    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Total_Population_Female) AS median_Total_Population_Female,
--    AVG(Total_Population_Female) AS mean_Total_Population_Female,
--    (MAX(Total_Population_Female) - MIN(Total_Population_Female)) AS range_Total_Population_Female,
    
--    MAX(Total_Population_Population) AS max_Total_Population_Population,
--    MIN(Total_Population_Population) AS min_Total_Population_Population,
--    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Total_Population_Population) AS median_Total_Population_Population,
--    AVG(Total_Population_Population) AS mean_Total_Population_Population,
--    (MAX(Total_Population_Population) - MIN(Total_Population_Population)) AS range_Total_Population_Population,

--  etc. 

-- FROM residents;


----------------------------------------------------------------------------------------------------------------------------#
-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )

-- No records appeared wildly exaggerated or improbable, and thus were deemed appropriate for staging.















-------------------------------------------------------------------#
-- Staging | This model is used to rename and stage the resident data from the raw schema.
-------------------------------------------------------------------#


-- Define the staging model for the population data.
-------------------------------------------------------------------#
{{
    config(
        materialized='view'
    )
}}



-- Step 1: Renaming feature headings into a readable format (requirement for Section 2b), & omitting duplicates.
-------------------------------------------------------------------#

SELECT
    LGA_CODE_2016 AS lga_code_2016,

    Tot_P_M AS Total_Population_Male,
    Tot_P_F AS Total_Population_Female,
    Tot_P_P AS Total_Population_Population,

    Age_0_4_yr_M AS Age_0_4_Year_Male,
    Age_0_4_yr_F AS Age_0_4_Year_Female,
    Age_0_4_yr_P AS Age_0_4_Year_Population,

    Age_5_14_yr_M AS Age_5_14_Year_Male,
    Age_5_14_yr_F AS Age_5_14_Year_Female,
    Age_5_14_yr_P AS Age_5_14_Year_Population,

    Age_15_19_yr_M AS Age_15_19_Year_Male,
    Age_15_19_yr_F AS Age_15_19_Year_Female,
    Age_15_19_yr_P AS Age_15_19_Year_Population,

    Age_20_24_yr_M AS Age_20_24_Year_Male,
    Age_20_24_yr_F AS Age_20_24_Year_Female,
    Age_20_24_yr_P AS Age_20_24_Year_Population,

    Age_25_34_yr_M AS Age_25_34_Year_Male,
    Age_25_34_yr_F AS Age_25_34_Year_Female,
    Age_25_34_yr_P AS Age_25_34_Year_Population,

    Age_35_44_yr_M AS Age_35_44_Year_Male,
    Age_35_44_yr_F AS Age_35_44_Year_Female,
    Age_35_44_yr_P AS Age_35_44_Year_Population,

    Age_45_54_yr_M AS Age_45_54_Year_Male,
    Age_45_54_yr_F AS Age_45_54_Year_Female,
    Age_45_54_yr_P AS Age_45_54_Year_Population,

    Age_55_64_yr_M AS Age_55_64_Year_Male,
    Age_55_64_yr_F AS Age_55_64_Year_Female,
    Age_55_64_yr_P AS Age_55_64_Year_Population,

    Age_65_74_yr_M AS Age_65_74_Year_Male,
    Age_65_74_yr_F AS Age_65_74_Year_Female,
    Age_65_74_yr_P AS Age_65_74_Year_Population,

    Age_75_84_yr_M AS Age_75_84_Year_Male,
    Age_75_84_yr_F AS Age_75_84_Year_Female,
    Age_75_84_yr_P AS Age_75_84_Year_Population,

    Age_85ov_M AS Age_85_Over_Male,
    Age_85ov_F AS Age_85_Over_Female,
    Age_85ov_P AS Age_85_Over_Population,

    Counted_Census_Night_home_M AS Counted_Census_Night_Home_Male,
    Counted_Census_Night_home_F AS Counted_Census_Night_Home_Female,
    Counted_Census_Night_home_P AS Counted_Census_Night_Home_Population,

    Count_Census_Nt_Ewhere_Aust_M AS Count_Census_Night_Elsewhere_Australia_Male,
    Count_Census_Nt_Ewhere_Aust_F AS Count_Census_Night_Elsewhere_Australia_Female,
    Count_Census_Nt_Ewhere_Aust_P AS Count_Census_Night_Elsewhere_Australia_Population,

    Indigenous_psns_Aboriginal_M AS Indigenous_Persons_Aboriginal_Male,
    Indigenous_psns_Aboriginal_F AS Indigenous_Persons_Aboriginal_Female,
    Indigenous_psns_Aboriginal_P AS Indigenous_Persons_Aboriginal_Population,

    Indig_psns_Torres_Strait_Is_M AS Indigenous_Persons_Torres_Strait_Islander_Male,
    Indig_psns_Torres_Strait_Is_F AS Indigenous_Persons_Torres_Strait_Islander_Female,
    Indig_psns_Torres_Strait_Is_P AS Indigenous_Persons_Torres_Strait_Islander_Population,

    Indig_Bth_Abor_Torres_St_Is_M AS Indigenous_Birth_Aboriginal_Torres_Strait_Islander_Male,
    Indig_Bth_Abor_Torres_St_Is_F AS Indigenous_Birth_Aboriginal_Torres_Strait_Islander_Female,
    Indig_Bth_Abor_Torres_St_Is_P AS Indigenous_Birth_Aboriginal_Torres_Strait_Islander_Population,

    Indigenous_P_Tot_M AS Indigenous_Population_Total_Male,
    Indigenous_P_Tot_F AS Indigenous_Population_Total_Female,
    Indigenous_P_Tot_P AS Indigenous_Population_Total_Population,

    Birthplace_Australia_M AS Birthplace_Australia_Male,
    Birthplace_Australia_F AS Birthplace_Australia_Female,
    Birthplace_Australia_P AS Birthplace_Australia_Population,

    Birthplace_Elsewhere_M AS Birthplace_Elsewhere_Male,
    Birthplace_Elsewhere_F AS Birthplace_Elsewhere_Female,
    Birthplace_Elsewhere_P AS Birthplace_Elsewhere_Population,

    Lang_spoken_home_Eng_only_M AS Language_Spoken_English_Only_Male,
    Lang_spoken_home_Eng_only_F AS Language_Spoken_English_Only_Female,
    Lang_spoken_home_Eng_only_P AS Language_Spoken_English_Only_Population,

    Lang_spoken_home_Oth_Lang_M AS Language_Spoken_Other_Language_Male,
    Lang_spoken_home_Oth_Lang_F AS Language_Spoken_Other_Language_Female,
    Lang_spoken_home_Oth_Lang_P AS Language_Spoken_Other_Language_Population,

    Australian_citizen_M AS Australian_Citizen_Male,
    Australian_citizen_F AS Australian_Citizen_Female,
    Australian_citizen_P AS Australian_Citizen_Population,

    Age_psns_att_educ_inst_0_4_M AS Age_Persons_Attending_Educational_Institution_0_4_Male,
    Age_psns_att_educ_inst_0_4_F AS Age_Persons_Attending_Educational_Institution_0_4_Female,
    Age_psns_att_educ_inst_0_4_P AS Age_Persons_Attending_Educational_Institution_0_4_Population,

    Age_psns_att_educ_inst_5_14_M AS Age_Persons_Attending_Educational_Institution_5_14_Male,
    Age_psns_att_educ_inst_5_14_F AS Age_Persons_Attending_Educational_Institution_5_14_Female,
    Age_psns_att_educ_inst_5_14_P AS Age_Persons_Attending_Educational_Institution_5_14_Population,

    Age_psns_att_edu_inst_15_19_M AS Age_Persons_Attending_Educational_Institution_15_19_Male,
    Age_psns_att_edu_inst_15_19_F AS Age_Persons_Attending_Educational_Institution_15_19_Female,
    Age_psns_att_edu_inst_15_19_P AS Age_Persons_Attending_Educational_Institution_15_19_Population,

    Age_psns_att_edu_inst_20_24_M AS Age_Persons_Attending_Educational_Institution_20_24_Male,
    Age_psns_att_edu_inst_20_24_F AS Age_Persons_Attending_Educational_Institution_20_24_Female,
    Age_psns_att_edu_inst_20_24_P AS Age_Persons_Attending_Educational_Institution_20_24_Population,

    Age_psns_att_edu_inst_25_ov_M AS Age_Persons_Attending_Educational_Institution_25_and_Over_Male,
    Age_psns_att_edu_inst_25_ov_F AS Age_Persons_Attending_Educational_Institution_25_and_Over_Female,
    Age_psns_att_edu_inst_25_ov_P AS Age_Persons_Attending_Educational_Institution_25_and_Over_Population,

    High_yr_schl_comp_Yr_12_eq_M AS High_School_Completion_Year_12_or_Equivalent_Male,
    High_yr_schl_comp_Yr_12_eq_F AS High_School_Completion_Year_12_or_Equivalent_Female,
    High_yr_schl_comp_Yr_12_eq_P AS High_School_Completion_Year_12_or_Equivalent_Population,

    High_yr_schl_comp_Yr_11_eq_M AS High_School_Completion_Year_11_or_Equivalent_Male,
    High_yr_schl_comp_Yr_11_eq_F AS High_School_Completion_Year_11_or_Equivalent_Female,
    High_yr_schl_comp_Yr_11_eq_P AS High_School_Completion_Year_11_or_Equivalent_Population,

    High_yr_schl_comp_Yr_10_eq_M AS High_School_Completion_Year_10_or_Equivalent_Male,
    High_yr_schl_comp_Yr_10_eq_F AS High_School_Completion_Year_10_or_Equivalent_Female,
    High_yr_schl_comp_Yr_10_eq_P AS High_School_Completion_Year_10_or_Equivalent_Population,

    High_yr_schl_comp_Yr_9_eq_M AS High_School_Completion_Year_9_or_Equivalent_Male,
    High_yr_schl_comp_Yr_9_eq_F AS High_School_Completion_Year_9_or_Equivalent_Female,
    High_yr_schl_comp_Yr_9_eq_P AS High_School_Completion_Year_9_or_Equivalent_Population,

    High_yr_schl_comp_Yr_8_belw_M AS High_School_Completion_Year_8_or_Below_Male,
    High_yr_schl_comp_Yr_8_belw_F AS High_School_Completion_Year_8_or_Below_Female,
    High_yr_schl_comp_Yr_8_belw_P AS High_School_Completion_Year_8_or_Below_Population,

    High_yr_schl_comp_D_n_g_sch_M AS High_School_Completion_Did_Not_Go_To_School_Male,
    High_yr_schl_comp_D_n_g_sch_F AS High_School_Completion_Did_Not_Go_To_School_Female,
    High_yr_schl_comp_D_n_g_sch_P AS High_School_Completion_Did_Not_Go_To_School_Population,

    Count_psns_occ_priv_dwgs_M AS Count_Persons_Occupying_Private_Dwellings_Male,
    Count_psns_occ_priv_dwgs_F AS Count_Persons_Occupying_Private_Dwellings_Female,
    Count_psns_occ_priv_dwgs_P AS Count_Persons_Occupying_Private_Dwellings_Population,

    Count_Persons_other_dwgs_M AS Count_Persons_in_Other_Dwellings_Male,
    Count_Persons_other_dwgs_F AS Count_Persons_in_Other_Dwellings_F


FROM raw.residents
