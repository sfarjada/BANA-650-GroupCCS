/*The following are Common Table Expressions, or CTE's. They facilitate smaller querying for better troubleshooting. The actual, full query is below the CTEs*/
WITH CDI_HADM_ID AS (
    SELECT 			   
   	 SUBJECT_ID,
   	 HADM_ID
    FROM
   	 `physionet-data`.mimiciii_clinical.diagnoses_icd
    WHERE
   	 ICD9_CODE = '00845'
   	 AND SUBJECT_ID NOT IN (256, 5637, 9976, 10774, 16994, 17722, 18649, 19823, 21734, 22026, 23039, 23761, 25225, 26200, 26274, 30924, 88662)
/*
gets each hospital admission (hadm_id) that resulted in a cdi; 1427 results. number of unique patients is 1294 (some got it on more than one occasion). the "not in" list is patients who don't have vitals and/or labs from their first icu stay, so were not included in our study (because the study is all about predicting cdi mortality from the first vitals and labs).
*/
),
Expire_Flag_Sort AS (
    SELECT
   	 c.SUBJECT_ID,
   	 c.HADM_ID,
   	 a.HOSPITAL_EXPIRE_FLAG
    FROM
   	 CDI_HADM_ID c
    LEFT JOIN
   	 `physionet-data`.mimiciii_clinical.admissions a ON c.HADM_ID = a.HADM_ID
/*
gets the hospital_expire_flag and associated hadm_id. if hospital_expire_flag = 1, the patient died while in the hospital during that hadm_id, 0 if they didn't die during that hospital admission. note: a 0 does not mean they never died, just that they didn't die at the hospital.
*/
),
Expire_Flag_Collapse AS (
    SELECT   		   
   	 SUBJECT_ID,
   	 MAX(HOSPITAL_EXPIRE_FLAG) AS inhospital_death -- alias bc of MAX
    FROM
   	 Expire_Flag_Sort
    GROUP BY
   	 SUBJECT_ID
/*
this cte uses the results of Expire_Flag_Sort with MAX to keep only the highest value in the hospital_expire_flag columns for each subject_id, which is either a 1 or 0 depending on whether the patient got a cdi during a hadm_id. the group by "collapses" the results into one row per subject_id so we can know if the patient ever died at the hospital.
*/
),
Comorbidities_Collapse AS (
    SELECT   				   
   	 SUBJECT_ID,
   	 MAX(CASE WHEN ICD9_CODE LIKE '250%' THEN 1 ELSE 0 END) AS diabetes,
   	 MAX(CASE WHEN ICD9_CODE LIKE '585%' THEN 1 ELSE 0 END) AS chronic_kidney_disease,
   	 MAX(CASE WHEN ICD9_CODE LIKE '414%' THEN 1 ELSE 0 END) AS chronic_ischemic_heart_disease
    FROM
   	 `physionet-data`.mimiciii_clinical.diagnoses_icd    
    GROUP BY
   	 SUBJECT_ID
/*
comorbidities are logged in the icd9_code column, leading to multiple rows with the same subject_id but different icd9 code. this cte spreads out the comorbidities into their own appropriately named columns, puts a 1 if they have that illness or 0 if they don't, and then collapses subject_id's so that there's only one row of comorbidities per subject_id.
*/
),
Genders AS (
    SELECT
	SUBJECT_ID,
   	 CASE
   		 WHEN LOWER(GENDER) = 'm' THEN 1
   		 WHEN LOWER(GENDER) = 'f' THEN 0
   		 ELSE NULL
   	 END AS gender_binaried
    FROM
   	 `physionet-data`.mimiciii_clinical.patients    
/*
gets patient's gender and converts to binary
*/
),
Labs_Sort AS (    
    SELECT    		 
   	 SUBJECT_ID,
   	 HADM_ID,
   	 ITEMID,
   	 VALUENUM,
   	 CHARTTIME,
   	 ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID, HADM_ID, ITEMID ORDER BY CHARTTIME) AS counter
    FROM
   	 `physionet-data`.mimiciii_clinical.labevents
    WHERE
   	 ITEMID IN (
   		 50868, 50862, 50882, 50885, 50912, 50902, 50809, 51221, 50811,
   		 50813, 51265, 50971, 51275, 51237, 50983, 51006, 51301, 50893, 50808
   	 )
/*
lab results are all listed in one column, so there'll be hundreds of rows with the same subject_id, hadm_id and charttime (time stamp), but with different lab type and its result. this cte marks the first lab results of each hadm_id with a 1 for use in the next cte. need to do all hadm_ids bc each hadm_id does not necessarily include an icu visit, and even it if did, that icu visit does not necessarily result in a cdi. later we will find the hadm_id of all icu visits to find which of these labs are part of hospital admission that resulted in a cdi.
*/
),
First_Labs AS (
    SELECT
   	 l.SUBJECT_ID,
   	 l.HADM_ID,
   	 MAX(CASE WHEN l.ITEMID = 50868 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS anion_gap,
   	 MAX(CASE WHEN l.ITEMID = 50862 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS albumin,
   	 MAX(CASE WHEN l.ITEMID = 50882 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS bicarbonate,
   	 MAX(CASE WHEN l.ITEMID = 50885 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS bilirubin_total,
   	 MAX(CASE WHEN l.ITEMID = 50912 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS creatinine,
   	 MAX(CASE WHEN l.ITEMID = 50902 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS chloride,
   	 MAX(CASE WHEN l.ITEMID = 50809 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS glucose_bloodgas,
   	 MAX(CASE WHEN l.ITEMID = 51221 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS hematocrit_blood,
   	 MAX(CASE WHEN l.ITEMID = 50811 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS hemoglobin_bloodgas,
   	 MAX(CASE WHEN l.ITEMID = 50813 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS lactate,
   	 MAX(CASE WHEN l.ITEMID = 51265 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS platelet_count,
   	 MAX(CASE WHEN l.ITEMID = 50971 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS potassium_blood,
   	 MAX(CASE WHEN l.ITEMID = 51275 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS ptt,
   	 MAX(CASE WHEN l.ITEMID = 51237 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS inr_pt,
   	 MAX(CASE WHEN l.ITEMID = 50983 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS sodium,
   	 MAX(CASE WHEN l.ITEMID = 51006 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS bun,
   	 MAX(CASE WHEN l.ITEMID = 51301 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS wbc_blood,
   	 MAX(CASE WHEN l.ITEMID = 50893 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS calcium_total,
   	 MAX(CASE WHEN l.ITEMID = 50808 AND l.counter = 1 THEN l.VALUENUM ELSE NULL END) AS free_calcium
    FROM
   	 Labs_Sort l
    WHERE
   	 l.counter = 1
    GROUP BY
   	 l.SUBJECT_ID, l.HADM_ID
/*
take results of Labs_Sort, expand each lab into their own appropriately named column, then collapse so there's one row of labs for each hadm_id. join this cte only after icu_stay hadm_id is found so sql knows which set of labs to extract.
*/
),
Vitals_Sort AS (
    SELECT    		    
   	 SUBJECT_ID,
   	 HADM_ID,
   	 ICUSTAY_ID,
   	 ITEMID,
   	 CHARTTIME,
   	 VALUENUM,
   	 ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID, HADM_ID, ITEMID ORDER BY CHARTTIME) AS counter
    FROM
   	 `physionet-data`.mimiciii_clinical.chartevents
    WHERE
   	 ITEMID IN (
   		 211, 618, 646, 678, 51, 8368, 52, 220045, 220210,
   		 223761, 225310, 220051, 225309, 220050, 225312, 220052
   	 )   	 
/*
like lab results, vital signs are listed in one column. the mit data dictionary says all these vitals are done in the icu, so presumably, the first vital signs of each subject_id here are the first vital signs of the patient's first icu stay. however, like in Labs_Sort, an icu visit does not necessarily result in a cdi. this cte marks the first vital signs of each hadm_id with a 1 for use in the next cte.
*/
),
First_Vitals AS (
    SELECT
   	 v.SUBJECT_ID,
   	 v.HADM_ID,
   	 MAX(CASE WHEN v.ITEMID = 211 AND v.counter = 1 THEN v.VALUENUM END) AS cv_heart_rate,
   	 MAX(CASE WHEN v.ITEMID = 618 AND v.counter = 1 THEN v.VALUENUM END) AS cv_respiratory_rate,
   	 MAX(CASE WHEN v.ITEMID = 678 AND v.counter = 1 THEN v.VALUENUM END) AS cv_body_temperature,
   	 MAX(CASE WHEN v.ITEMID = 51 AND v.counter = 1 THEN v.VALUENUM END) AS cv_systolic_blood_pressure,
   	 MAX(CASE WHEN v.ITEMID = 8368 AND v.counter = 1 THEN v.VALUENUM END) AS cv_diastolic_blood_pressure,
   	 MAX(CASE WHEN v.ITEMID = 52 AND v.counter = 1 THEN v.VALUENUM END) AS cv_mean_arterial_pressure,
   	 MAX(CASE WHEN v.ITEMID = 646 AND v.counter = 1 THEN v.VALUENUM END) AS spo2,
   	 MAX(CASE WHEN v.ITEMID = 220045 AND v.counter = 1 THEN v.VALUENUM END) AS mv_heart_rate,
   	 MAX(CASE WHEN v.ITEMID = 220210 AND v.counter = 1 THEN v.VALUENUM END) AS mv_respiratory_rate,
   	 MAX(CASE WHEN v.ITEMID = 223761 AND v.counter = 1 THEN v.VALUENUM END) AS mv_body_temperature,
   	 MAX(CASE WHEN v.ITEMID = 225310 AND v.counter = 1 THEN v.VALUENUM END) AS mv_diastolic_blood_pressure_1,
   	 MAX(CASE WHEN v.ITEMID = 220051 AND v.counter = 1 THEN v.VALUENUM END) AS mv_diastolic_blood_pressure_2,
   	 MAX(CASE WHEN v.ITEMID = 225309 AND v.counter = 1 THEN v.VALUENUM END) AS mv_systolic_blood_pressure_1,
   	 MAX(CASE WHEN v.ITEMID = 220050 AND v.counter = 1 THEN v.VALUENUM END) AS mv_systolic_blood_pressure_2,
   	 MAX(CASE WHEN v.ITEMID = 225312 AND v.counter = 1 THEN v.VALUENUM END) AS mv_mean_arterial_pressure_1,
   	 MAX(CASE WHEN v.ITEMID = 220052 AND v.counter = 1 THEN v.VALUENUM END) AS mv_mean_arterial_pressure_2
    FROM
   	 Vitals_Sort v
    WHERE
   	 v.counter = 1
    GROUP BY
   	 v.SUBJECT_ID, v.HADM_ID
/*
take results of Vitals_Sort, expand each vital into their own appropriately named column, then collapse so there's one row of vitals for each hadm_id. again, join this cte only after icu_stay hadm_id is found.
*/
),
ICU_Sort AS (
    SELECT
   	 SUBJECT_ID,
   	 HADM_ID, -- not used in this cte, but will be used later, so it needs to be here now
   	 INTIME,
   	 ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID ORDER BY INTIME) AS counter
    FROM
   	 `physionet-data`.mimiciii_clinical.icustays
/*
mark each patient's first icu visit with a 1, so that the hadm_id in that row can be extracted in the next cte.
*/
),
First_ICU AS (
    SELECT
   	 SUBJECT_ID,
   	 HADM_ID
    FROM
   	 ICU_Sort
    WHERE
   	 counter = 1
/*
take results of ICU_Sort and make a list of each patient and the hadm_id of their first icu visit. The hadm_ids found here will be used to associate labs, vitals, and age.
*/
),
Ages AS (
    SELECT
	i.HADM_ID,
   	 (DATE_DIFF(a.ADMITTIME, p.DOB, YEAR)) AS age
    FROM
   	 First_ICU i
    LEFT JOIN
   	 `physionet-data`.mimiciii_clinical.admissions a ON i.HADM_ID = a.HADM_ID
    LEFT JOIN
   	 `physionet-data`.mimiciii_clinical.patients p ON i.SUBJECT_ID = p.SUBJECT_ID
    /*
    WHERE (DATE_DIFF(a.ADMITTIME, p.DOB, YEAR)) > 16 AND (DATE_DIFF(a.ADMITTIME, p.DOB, YEAR)) < 90
    */
/*
the paper says they only kept ages between 16 and 90, but if we do that, we lose 105 patients (~8% of data). that's why the age filter is commented out
*/
),
Base_Query AS (
    SELECT
   	 e.SUBJECT_ID,
   	 g.gender_binaried,
   	 e.inhospital_death,
   	 cc.diabetes,
   	 cc.chronic_kidney_disease,
   	 cc.chronic_ischemic_heart_disease,
   	 i.HADM_ID
    FROM
   	 Expire_Flag_Collapse e
    LEFT JOIN
   	 Genders g ON e.SUBJECT_ID = g.SUBJECT_ID
    LEFT JOIN
   	 Comorbidities_Collapse cc ON e.SUBJECT_ID = cc.SUBJECT_ID
    LEFT JOIN
   	 First_ICU i ON e.SUBJECT_ID = i.SUBJECT_ID    -- hadm_id of the first icu visit
/*
1294 results. this is the simplest combined table to make, so do this first and then join the more database-intensive hadm_id associated stuff.
*/    
)

-- Finally, create the data set
SELECT
    b.SUBJECT_ID,
    b.gender_binaried,
    a.age,
    b.inhospital_death,
    b.diabetes,
    b.chronic_kidney_disease,
    b.chronic_ischemic_heart_disease,
    l.anion_gap,
    l.albumin,
    l.bicarbonate,
    l.bilirubin_total,
    l.creatinine,
    l.chloride,
    l.glucose_bloodgas,
    l.hematocrit_blood,
    l.hemoglobin_bloodgas,
    l.lactate,
    l.platelet_count,
    l.potassium_blood,
    l.ptt,
    l.inr_pt,
    l.sodium,
    l.bun,
    l.wbc_blood,
    l.calcium_total,
    l.free_calcium,
    v.cv_heart_rate,
    v.cv_respiratory_rate,
    ROUND(v.cv_body_temperature, 1) AS cv_body_temp,
    v.cv_systolic_blood_pressure,
    v.cv_diastolic_blood_pressure,
    v.cv_mean_arterial_pressure,
    v.spo2,
    v.mv_heart_rate,
    v.mv_respiratory_rate,
    ROUND(v.mv_body_temperature, 1) AS mv_body_temp,
    v.mv_diastolic_blood_pressure_1,
    v.mv_diastolic_blood_pressure_2,
    v.mv_systolic_blood_pressure_1,
    v.mv_systolic_blood_pressure_2,
    v.mv_mean_arterial_pressure_1,
    v.mv_mean_arterial_pressure_2    
FROM
    Base_Query b
LEFT JOIN
    FIRST_Labs l ON b.HADM_ID = l.HADM_ID
LEFT JOIN
    First_Vitals v ON b.HADM_ID = v.HADM_ID
    JOIN Ages a ON b.HADM_ID = a.HADM_ID
