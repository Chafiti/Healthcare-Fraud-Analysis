SELECT *
FROM inpatientdata;

-- Created duplicate table of data to safely clean it without altering original for inpatientdata
CREATE TABLE inpatientdata_staging
LIKE inpatientdata;

INSERT inpatientdata_staging
SELECT *
FROM inpatientdata;


SELECT *
FROM fraud_claims;

-- Created duplicate table of data to safely clean it without altering original for fraud claims
CREATE TABLE fraud_staging
LIKE fraud_claims;

INSERT fraud_staging
SELECT *
FROM fraud_claims;



-- Creates row that tracks the amount of duplicate rows
SELECT *,
ROW_NUMBER() OVER(PARTITION BY BeneID, claimID, ClaimStartDt, ClaimEndDt, Provider, InscClaimAmtReimbursed, AttendingPhysician, OperatingPhysician, OtherPhysician, AdmissionDt, ClmAdmitDiagnosisCode, DeductibleAmtPaid, DiagnosisGroupCode, 
ClmDiagnosisCode_1, ClmDiagnosisCode_2, ClmDiagnosisCode_3, ClmDiagnosisCode_4, ClmDiagnosisCode_5, ClmDiagnosisCode_6, ClmDiagnosisCode_7, ClmDiagnosisCode_8, ClmDiagnosisCode_9, ClmDiagnosisCode_10, ClmProcedureCode_1, ClmProcedureCode_2,
ClmProcedureCode_3, ClmProcedureCode_4, ClmProcedureCode_5, ClmProcedureCode_6) AS row_num
FROM inpatientdata_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY BeneID, claimID, ClaimStartDt, ClaimEndDt, Provider, InscClaimAmtReimbursed, AttendingPhysician, OperatingPhysician, OtherPhysician, AdmissionDt, ClmAdmitDiagnosisCode, DeductibleAmtPaid, DiagnosisGroupCode, 
ClmDiagnosisCode_1, ClmDiagnosisCode_2, ClmDiagnosisCode_3, ClmDiagnosisCode_4, ClmDiagnosisCode_5, ClmDiagnosisCode_6, ClmDiagnosisCode_7, ClmDiagnosisCode_8, ClmDiagnosisCode_9, ClmDiagnosisCode_10, ClmProcedureCode_1, ClmProcedureCode_2,
ClmProcedureCode_3, ClmProcedureCode_4, ClmProcedureCode_5, ClmProcedureCode_6) AS row_num
FROM inpatientdata_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM inpatientdata_staging;


SELECT BeneID, TRIM(BeneID)
FROM inpatientdata_staging
WHERE BeneID != TRIM(BeneID);

SELECT DeductibleAmtPaid
FROM inpatientdata_staging
WHERE DeductibleAmtPaid != 1068;

SELECT ClaimStartDt
FROM inpatientdata_staging
ORDER BY 1;

-- Normalizing date text to promote data type to date
UPDATE inpatientdata_staging
SET ClaimStartDt = str_to_date(ClaimStartDt, '%Y-%m-%d');

ALTER TABLE inpatientdata_staging
MODIFY COLUMN DischargeDt DATE;

SELECT *
FROM inpatientdata_staging
WHERE DeductibleAmtPaid IS NULL;

-- Deletes unnecessary rows from fraud claims
DELETE FROM fraud_staging
WHERE Provider = 'Provider';

-- Joins inpatientdata and fraud claims
SELECT *
FROM inpatientdata_staging AS i
JOIN fraud_staging AS f
ON i.Provider = f.Provider;

-- Lists all instances of potential fraud
SELECT *
FROM inpatientdata_staging AS i
JOIN fraud_staging AS f
ON i.Provider = f.Provider
WHERE PotentialFraud = 'Yes';

-- Lists potential fraud claims for Providers who have multiple patients
WITH FraudProviders AS (
    SELECT 
        i.Provider,
        COUNT(*) AS ProviderCount
    FROM inpatientdata_staging AS i
    JOIN fraud_staging AS f
        ON i.Provider = f.Provider
    WHERE f.PotentialFraud = 'Yes'
    GROUP BY i.Provider
    HAVING COUNT(*) > 1
)
SELECT i.*
FROM inpatientdata_staging i
JOIN FraudProviders fp ON i.Provider = fp.Provider;

-- Search for individual Provider
SELECT *
FROM inpatientdata_staging AS i
JOIN fraud_staging AS f
ON i.Provider = f.Provider
WHERE i.Provider = 'PRV52019';

-- Lists patient count per Provider
SELECT Provider, COUNT(*) AS ProviderCount
FROM inpatientdata_staging
GROUP BY Provider
ORDER BY 2 DESC;


-- Days admitted vs Potential Fraud
SELECT 
  i.BeneID,
  i.ClaimEndDt,
  i.ClaimStartDt,
  DATEDIFF(i.ClaimEndDt, i.ClaimStartDt) AS DaysAdmitted,
  f.PotentialFraud
FROM inpatientdata_staging AS i
JOIN fraud_staging AS f
  ON i.Provider = f.Provider
ORDER BY DaysAdmitted DESC;

-- Cost Per Stay
SELECT BeneID, ClaimEndDt, ClaimStartDt,
  DATEDIFF(ClaimEndDt, ClaimStartDt) AS DaysAdmitted, InscClaimAmtReimbursed
FROM inpatientdata_staging
ORDER BY 4 DESC;


-- Average cost per provider with mutliple patients
SELECT Provider, 
       AVG(InscClaimAmtReimbursed) AS avg_claim,
       COUNT(*) AS patient_count
FROM inpatientdata_staging
GROUP BY Provider
HAVING COUNT(*) > 1
ORDER BY avg_claim DESC;


SELECT *
FROM inpatientdata_staging
WHERE Provider = 'PRV52845';

-- Lists Most frequent diagnosis group codes
SELECT DiagnosisGroupCode, COUNT(*) AS diagnosis_count
FROM inpatientdata_staging
GROUP BY DiagnosisGroupCode
ORDER BY 2 DESC;


SELECT Provider,
CASE
	WHEN OperatingPhysician != 'NA' AND OtherPhysician != 'NA' THEN '2 Associates'
    WHEN OperatingPhysician != 'NA' AND OtherPhysician ='NA' THEN '1 Associate'
    WHEN OperatingPhysician = 'NA' AND OtherPhysician !='NA' THEN '1 Associate'
    WHEN OperatingPhysician = 'NA' AND OtherPhysician ='NA' THEN 'No Associate'
    END AS accomplice
FROM inpatientdata_staging;

-- Lists only providers who have been labeled with potential fraud by physician count
SELECT 
  i.Provider,
  CASE
    WHEN i.OperatingPhysician != 'NA' AND i.OtherPhysician != 'NA' THEN '2 Associates'
    WHEN i.OperatingPhysician != 'NA' AND i.OtherPhysician = 'NA' THEN '1 Associate'
    WHEN i.OperatingPhysician = 'NA' AND i.OtherPhysician != 'NA' THEN '1 Associate'
    WHEN i.OperatingPhysician = 'NA' AND i.OtherPhysician = 'NA' THEN 'No Associate'
  END AS accomplice,
  f.PotentialFraud
FROM inpatientdata_staging AS i
JOIN fraud_staging AS f
  ON i.Provider = f.Provider
WHERE f.PotentialFraud = 'Yes';


-- Amount of potential fraud counted
SELECT PotentialFraud, COUNT(*) AS count
FROM fraud_staging
GROUP BY PotentialFraud;


-- Percentage of potential fraud based on amount of Physicians assigned to patient
WITH ProviderAssociates AS (
  SELECT 
    Provider,
    CASE
      WHEN MAX(OperatingPhysician) != 'NA' AND MAX(OtherPhysician) != 'NA' THEN '2 Associates'
      WHEN MAX(OperatingPhysician) != 'NA' OR MAX(OtherPhysician) != 'NA' THEN '1 Associate'
      ELSE 'No Associate'
    END AS accomplice_group
  FROM inpatientdata_staging
  GROUP BY Provider
),
LabeledProviders AS (
  SELECT 
    pa.accomplice_group,
    f.PotentialFraud
  FROM ProviderAssociates pa
  JOIN fraud_staging f ON pa.Provider = f.Provider
)
SELECT 
  accomplice_group,
  ROUND(SUM(CASE WHEN PotentialFraud = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_percentage
FROM LabeledProviders
GROUP BY accomplice_group
ORDER BY FIELD(accomplice_group, 'No Associate', '1 Associate', '2 Associates');


-- Fraud based on if an operation was done
WITH procedure_done AS(
SELECT Provider, 
CASE
	WHEN ClmProcedureCode_1 != 'NA' THEN 'Procedure done'
    ELSE 'No procedure'
    END AS operation
FROM inpatientdata_staging
)
SELECT pd.operation,
f.PotentialFraud
FROM procedure_done pd
JOIN fraud_staging f ON pd.Provider = f.Provider;


-- Counts amount of potential fraud based on if a procedure was done
WITH procedure_by_provider AS (
  SELECT 
    Provider,
    CASE
      WHEN MAX(ClmProcedureCode_1) != 'NA' THEN 'Procedure done'
      ELSE 'No procedure'
    END AS operation
  FROM inpatientdata_staging
  GROUP BY Provider
),
fraud_flags AS (
  SELECT Provider, PotentialFraud
  FROM fraud_staging
)
SELECT 
  pbp.operation,
  f.PotentialFraud,
  COUNT(*) AS provider_count
FROM procedure_by_provider pbp
JOIN fraud_flags f ON pbp.Provider = f.Provider
GROUP BY pbp.operation, f.PotentialFraud
ORDER BY pbp.operation, f.PotentialFraud;


SELECT COUNT(DISTINCT Provider)
FROM inpatientdata_staging;

