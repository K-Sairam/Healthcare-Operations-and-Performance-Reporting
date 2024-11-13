
--1. Display top 5 patients who are recently admitted
WITH RankedAdmissions AS
(
    SELECT 
		a.Patient_ID,
		a.Admission_Date,
		p.Name,
		RANK() OVER (ORDER BY a.admission_date DESC) AS ranking
    FROM dbo.tbl_admissions AS a
    JOIN dbo.tbl_patient_names AS p
    ON a.patient_id = p.patient_id
)
SELECT 
	Patient_ID, 
	Name, 
	Admission_Date
FROM RankedAdmissions
WHERE ranking<=5

--2. Display hospitals with more than 120 beds

SELECT 
	Hospital_ID,
	Hospital_Name,
	BedCount
FROM dbo.tbl_hopsital_mapping
WHERE BedCount>120
ORDER BY BedCount DESC

--3. Display patients who have admitted before 15th Aug 2020.

SELECT 
    a.Patient_ID, 
    p.Name, 
    a.Admission_Date
FROM dbo.tbl_admissions AS a
JOIN dbo.tbl_patient_names AS p
    ON a.Patient_ID = p.Patient_ID
WHERE CAST(a.Admission_Date AS DATE) < CAST('2020-08-15' AS DATE)



--4. Display the date for next TUESDAY from today's date?

DECLARE @Today DATE = GETDATE()
DECLARE @NextTuesday DATE = DATEADD(DAY, (9 - DATEPART(WEEKDAY, @Today) + 1) % 7, @Today)
SELECT @NextTuesday AS Next_Tuesday



--5. Display all the records in admissions table where patients joined after 21-Aug-2020 and before 27-Aug-2020?


SELECT * 
FROM dbo.tbl_admissions 
WHERE TRY_CAST(admission_date AS DATE) BETWEEN '2020-08-21' AND '2020-08-27';


--select TRY_CONVERT(DATE,'8-7-20 19:00') 
--SELECT CAST('8-7-20 19:00' AS DATETIME2) 1297 916

--6. Display all hospitals in East region along with the name of hospital

SELECT 
	Hospital_ID, 
	Hospital_Name, 
	Region
FROM dbo.tbl_hopsital_mapping
WHERE Region = 'East'

--7. Display the list of all Hospitals whose no. of beds are greater the average no. of beds in hospitals

WITH AverageBeds AS (
    SELECT AVG(hm.BedCount) AS Avg_Beds
    FROM dbo.tbl_hopsital_mapping AS hm
)
SELECT hm.Hospital_Name, hm.BedCount, ab.Avg_Beds
FROM dbo.tbl_hopsital_mapping AS hm
CROSS JOIN AverageBeds AS ab
WHERE hm.BedCount > ab.Avg_Beds
ORDER BY BedCount DESC

--8. Display all patients whose name starts with N and ends with S
SELECT Name FROM dbo.tbl_patient_names
WHERE Name LIKE 'N%' AND Name LIKE '%S'

--9. Display all names of the patients whose first character could be anything, but second character should be L?
SELECT Name FROM dbo.tbl_patient_names
WHERE Name LIKE '_L%';

--10. Display Staff name, role and hospital name from labor table with format of {Staff name} works as {role} in {hospital name} hospital
SELECT DISTINCT
    CONCAT(l.Name, ' works as ', l.Position, ' in ', h.Hospital_Name, ' hospital') AS Staff_Info
FROM dbo.tbl_labor_hours AS l
JOIN dbo.tbl_hopsital_mapping AS h
ON l.Hospital_ID = h.Hospital_ID

SELECT Name, COUNT(*)
FROM dbo.tbl_labor_hours
GROUP BY Name, Hospital_ID
HAVING COUNT(*) > 1;

--11. Display patient name, admission type from admissions table. Also add another column in the same 
--query and it should display 1 for Actual Admission, 0 for Readmission. (Use Case Statement)
SELECT
    p.Name AS Patient_Name,
    a.Action_Type,
    CASE
        WHEN a.Action_Type = 'ActualAdmission' THEN 1
        WHEN a.Action_Type = 'Readmission' THEN 0
        ELSE NULL
    END AS Admission_Status
FROM dbo.tbl_admissions AS a
JOIN dbo.tbl_patient_names AS p
ON a.Patient_ID = p.Patient_ID;

--12. Display the list of patients along with their admission date, for those who have joined in first week of August?
SELECT
    p.Name AS Patient_Name,
    a.Admission_Date
FROM dbo.tbl_admissions AS a
JOIN dbo.tbl_patient_names AS p
ON a.Patient_ID = p.Patient_ID
WHERE DATEPART(MONTH, a.Admission_Date) = 8
  AND DATEPART(DAY, a.Admission_Date) BETWEEN 1 AND 7
  AND DATEPART(YEAR, a.Admission_Date) = 2020

--13. Display who has most time spent and least time spent patient in hospital?

WITH StayDuration AS(
    SELECT
        a.Patient_ID,
        p.Name AS Patient_Name,
        ABS(DATEDIFF(DAY, a.Admission_Date, d.Discharge_Date)) AS Days_Spent
    FROM dbo.tbl_admissions AS a
    JOIN dbo.tbl_patient_names AS p
	ON a.Patient_ID = p.Patient_ID
	JOIN dbo.tbl_discharges as d
	ON p.Patient_ID = d.Patient_ID
--	WHERE DATEDIFF(DAY, a.Admission_Date, d.Discharge_Date) >0
)
SELECT DISTINCT
    Patient_Name,
    Days_Spent
FROM StayDuration
WHERE Days_Spent = (SELECT MAX(Days_Spent) FROM StayDuration) 
   OR Days_Spent = (SELECT MIN(Days_Spent) FROM StayDuration)
ORDER BY Days_Spent DESC


--14. Display the patient details and his hospital details and assign a severity grade based on their number of days admitted.

WITH PatientStay AS (
    SELECT
        a.Patient_ID,
        p.Name AS Patient_Name,
        h.Hospital_Name,
        ABS(DATEDIFF(DAY, a.Admission_Date, d.Discharge_Date)) AS Days_Admitted
    FROM dbo.tbl_admissions AS a
    JOIN dbo.tbl_patient_names AS p
    ON a.Patient_ID = p.Patient_ID
    JOIN dbo.tbl_hopsital_mapping AS h
    ON a.Hospital_ID = h.Hospital_ID
	JOIN dbo.tbl_discharges as d
	ON h.Hospital_ID = d.Hospital_ID
	--WHERE DATEDIFF(day, a.Admission_Date, d.Discharge_Date)>0
)

SELECT DISTINCT
    Patient_Name,
    Hospital_Name,
    Days_Admitted,
    CASE
        WHEN Days_Admitted > 30 THEN 'High'
        WHEN Days_Admitted BETWEEN 15 AND 30 THEN 'Medium'
        WHEN Days_Admitted BETWEEN 15 AND 0 THEN 'Low'
        ELSE 'Unknown'
    END AS Severity_Grade
FROM PatientStay
ORDER BY Days_Admitted DESC


--15. Display the number of staff members in each position/role

SELECT
    COUNT(*) AS Number_of_Staff,
	Position
FROM dbo.tbl_labor_hours
GROUP BY Position

--16.Increase the wage for Nurses by 10% extra for all the employees who have worked for more than 10 hours a day as bonus for that day.

SELECT 
    w.Postion AS Position,
    w.Hourly_Wage____hr_ AS Current_Wage,
    CASE
        WHEN l.Position = 'Nurse' AND l.Hours > 10 THEN CAST(w.Hourly_Wage____hr_ * 1.10 AS INT)
        ELSE w.Hourly_Wage____hr_
    END AS Adjusted_Wage
FROM dbo.tbl_hourly_wages AS w
LEFT JOIN dbo.tbl_labor_hours AS l
ON w.Postion = l.Position
--SELECT DISTINCT POSItion from dbo.tbl_labor_hours

--===============================
--17. Update patient id 16577 admission type same as 2153 admission type.
SELECT
    Patient_ID,
    CASE
        WHEN Patient_ID = 16577 THEN(
            SELECT Action_Type AS Admission_Type
            FROM dbo.tbl_admissions
            WHERE Patient_ID = 2153
        )
        ELSE Action_Type
    END AS Admission_Type
FROM dbo.tbl_admissions
--ORDER BY Patient_ID DESC

--=========================================
--18. Display all the records in admission table along with the Row No. (Serial No.)
SELECT
	* ,
	ROW_NUMBER() OVER (ORDER BY Admission_Date) AS Row_No
FROM dbo.tbl_admissions

--19. Display all the databases, tables and their constraints on each table using information schema

SELECT schema_name AS database_name
FROM information_schema.schemata;

-- List all tables and their constraints for each database
SELECT 
    t.table_schema AS database_name,
    t.table_name,
    c.constraint_type,
    c.constraint_name
FROM 
    information_schema.tables t
LEFT JOIN 
    information_schema.table_constraints c
ON 
    t.table_schema = c.table_schema 
    AND t.table_name = c.table_name
WHERE 
    t.table_type = 'BASE TABLE'
ORDER BY 
    t.table_schema, t.table_name;


--20. Display how many primary keys, unique keys and foreign keys present on each table in Tech On boarding Database
-- CTE to count primary keys
WITH PrimaryKeyCounts AS (
    SELECT 
        tc.TABLE_NAME,
        COUNT(*) AS Primary_Keys
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
    WHERE 
        tc.TABLE_SCHEMA = 'Tech_Onboarding_Dat'  
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'  
    GROUP BY 
        tc.TABLE_NAME
),

-- CTE to count foreign keys
ForeignKeyCounts AS (
    SELECT 
        tc.TABLE_NAME,
        COUNT(*) AS Foreign_Keys
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
    WHERE 
        tc.TABLE_SCHEMA = 'Tech_Onboarding_Data'  
        AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'  
    GROUP BY 
        tc.TABLE_NAME
),

-- CTE to count unique keys
UniqueKeyCounts AS (
    SELECT 
        tc.TABLE_NAME,
        COUNT(*) AS Unique_Keys
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
    WHERE 
        tc.TABLE_SCHEMA = 'Tech_Onboarding_Data'  
        AND tc.CONSTRAINT_TYPE = 'UNIQUE'  
    GROUP BY 
        tc.TABLE_NAME
)

-- Main query to list tables with constraint counts
SELECT 
    t.TABLE_NAME,
    COALESCE(pk.Primary_Keys, 0) AS Primary_Keys,  
    COALESCE(fk.Foreign_Keys, 0) AS Foreign_Keys,  
    COALESCE(uk.Unique_Keys, 0) AS Unique_Keys    
FROM 
    INFORMATION_SCHEMA.TABLES AS t
LEFT JOIN 
    PrimaryKeyCounts AS pk ON t.TABLE_NAME = pk.TABLE_NAME
LEFT JOIN 
    ForeignKeyCounts AS fk ON t.TABLE_NAME = fk.TABLE_NAME
LEFT JOIN 
    UniqueKeyCounts AS uk ON t.TABLE_NAME = uk.TABLE_NAME





--========================================
--21. Display how many tables, views and stored procedures are present in the database.

SELECT
    'Tables' AS Object_Type,
    COUNT(*) AS Count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT
    'Views' AS Object_Type,
    COUNT(*) AS Count
FROM INFORMATION_SCHEMA.VIEWS
UNION ALL
SELECT
    'Stored Procedures' AS Object_Type,
    COUNT(*) AS Count
FROM sys.objects


--22. Display how many different types of data types are present in the tables used

SELECT DISTINCT
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS

--23. Display how many servers are linked to the server you are using.

SELECT COUNT(*) AS Linked_Servers
FROM sys.servers
WHERE is_linked = 1
--SELECT * FROM sys.servers
--WHERE is_linked = 1
--===============================

--24. Display what are the type of objects present

SELECT name AS ObjectName, type_desc AS ObjectType
FROM   sys.objects

---------------------------------------------------------

SELECT 
    tbl_patient_names.Patient_ID,
    -- Extract First Name by removing known prefixes
    LTRIM(
        CASE 
            -- Look for the first space to determine if there's a prefix
            WHEN CHARINDEX(' ', Name) > 0 THEN 
                REPLACE(
                    REPLACE(
                        REPLACE(
                            SUBSTRING(Name, CHARINDEX(' ', Name) + 1, LEN(Name)), 
                            'smt.', ''
                        ), 
                        'mr.', ''
                    ), 
                    'ms.', ''
                )
            ELSE 
                REPLACE(Name, '@', '') -- Return Name as First Name if no space
        END
    ) AS First_Name,

    -- Extract Last Name
    CASE 
        WHEN CHARINDEX(' ', Name) > 0 THEN 
            REPLACE(
                REPLACE(
                    REPLACE(
                        SUBSTRING(Name, 1, CHARINDEX(' ', Name) - 1), 
                        'smt.', ''
                    ), 
                    'mr.', ''
                ), 
                'ms.', ''
            )
        ELSE 
            '' -- If no space, return empty string for Last_Name
    END AS Last_Name
FROM dbo.tbl_patient_names
WHERE PATINDEX('%[^a-zA-Z ]%', Name) = 0 -- Filter to ensure only alphabetic names
   OR CHARINDEX('smt.', Name) > 0 
   OR CHARINDEX('mr.', Name) > 0 
   OR CHARINDEX('ms.', Name) > 0


--b.
SELECT 
    Patient_ID,
    Name,
    DOB,
    DATEDIFF(YEAR, DOB, GETDATE()) AS age
FROM dbo.tbl_patient_names
select * from tbl_hopsital_mapping
--c.

SELECT DISTINCT *
INTO #temp_mapping
FROM tbl_hopsital_mapping;
WITH CTE AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Hospital_Id ORDER BY (SELECT NULL)) AS rn
    FROM 
        #temp_mapping
)
DELETE FROM CTE
WHERE rn > 1;

SELECT * FROM #temp_mapping



--d. Review current column data types
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = 'Tech_Onboarding_data';

------------------------------------------------------------------------

-- a. Calculate the age and bucket them into age ranges
SELECT
    p.Patient_ID,
	REPLACE(p.Name, '@', '') AS Cleaned_Name,
    DATEDIFF(YEAR, p.DOB, GETDATE()) AS Age,
    CASE 
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 1 AND 10 THEN '1-10'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 11 AND 20 THEN '11-20'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 21 AND 30 THEN '21-30'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 31 AND 40 THEN '31-40'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 41 AND 50 THEN '41-50'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 51 AND 60 THEN '51-60'
        WHEN DATEDIFF(YEAR, p.DOB, GETDATE()) BETWEEN 61 AND 70 THEN '61-70'
        ELSE '71+' 
    END AS Age_Bucket
FROM dbo.tbl_patient_names AS p

-- b. Find the shift in which they have been admitted, 8 hour shifts
SELECT
    a.Patient_ID,
    a.Admission_Date,
    CASE 
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 0 AND 7 THEN '00:01-08:00'
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 8 AND 15 THEN '08:01-16:00'
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 16 AND 23 THEN '16:01-00:00'
        ELSE 'Unknown Shift'
    END AS Shift_Bucket,
    CASE 
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 0 AND 7 THEN 'Night'
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 8 AND 15 THEN 'Morning'
        WHEN DATEPART(HOUR, a.Admission_Date) BETWEEN 16 AND 23 THEN 'Afternoon'
        ELSE 'Unknown Shift'
    END AS Shift_Name
FROM dbo.tbl_admissions AS a;


--c. Find out the length of stay for each patient


WITH StayDuration AS(
    SELECT
        a.Patient_ID,
        REPLACE(p.Name, '@', '') AS Patient_Name,
        ABS(DATEDIFF(DAY, CAST(a.Admission_Date AS date), CAST(d.Discharge_Date AS date))) AS Days_Spent
    FROM dbo.tbl_admissions AS a
    JOIN dbo.tbl_patient_names AS p
	ON a.Patient_ID = p.Patient_ID
	JOIN dbo.tbl_discharges as d
	ON p.Patient_ID = d.Patient_ID
--	WHERE DATEDIFF(DAY, a.Admission_Date, d.Discharge_Date) >0
)
SELECT DISTINCT
    Patient_Name,
    Days_Spent
FROM StayDuration
--d. Find out average length of stay for each facility
SELECT
    h.Hospital_Name,
    ABS(AVG(DATEDIFF(DAY, a.Admission_Date, d.Discharge_Date))) AS Avg_Length_of_Stay
FROM dbo.tbl_admissions AS a
JOIN dbo.tbl_discharges AS d
    ON a.Patient_ID = d.Patient_ID
JOIN dbo.tbl_hopsital_mapping AS h
    ON a.Hospital_ID = h.Hospital_ID
GROUP BY h.Hospital_Name;
-------------------------------------------------------------------------------------------

--e

WITH StayDurations AS (
    SELECT
        im.Insurance_Company,
        ABS(DATEDIFF(DAY, Admission_Date, Discharge_Date)) AS Length_of_Stay
    FROM dbo.tbl_insurance_mapping AS im
	JOIN dbo.tbl_patient_census AS pc ON im.Insurance_Code = pc.PayerCode
	JOIN dbo.tbl_admissions AS a ON a.Patient_ID=pc.ResidentNumber
	JOIN dbo.tbl_discharges AS d On d.Patient_ID=pc.ResidentNumber
 --   WHERE Discharge_Date IS NOT NULL AND DATEDIFF(DAY, Admission_Date, Discharge_Date) > 0
)
SELECT
    Insurance_Company,
    MAX(Length_of_Stay) AS Longest_Stay,
    MIN(Length_of_Stay) AS Shortest_Stay
FROM StayDurations
GROUP BY Insurance_Company;

--f. Calculate the percentage of patients using each insurance type
--SELECT * FROM tbl_admissions

------------------
WITH PatientCounts AS (
    SELECT
        im.Insurance_Company,
        COUNT(DISTINCT pc.ResidentNumber) AS Patient_Count
    FROM dbo.tbl_insurance_mapping AS im
    JOIN dbo.tbl_patient_census AS pc ON im.Insurance_Code = pc.PayerCode
    JOIN dbo.tbl_admissions AS a ON a.Admission_Date = pc.CensusDate
    GROUP BY im.Insurance_Company
),
TotalPatientCounts AS (
    SELECT SUM(Patient_Count) AS Total_Count
    FROM PatientCounts
)
SELECT
    pc.Insurance_Company,
    pc.Patient_Count,
    ROUND(CAST(pc.Patient_Count AS FLOAT) / CAST(tp.Total_Count AS FLOAT) * 100, 2) AS Percentage
FROM PatientCounts AS pc
CROSS JOIN TotalPatientCounts AS tp
ORDER BY Percentage DESC; 


--g.  Calculate the number of unique admits per month
SELECT
    MONTH(Admission_Date) AS Month,
    COUNT(DISTINCT Patient_ID) AS Unique_Admits
FROM dbo.tbl_insurance_mapping AS im
	JOIN dbo.tbl_patient_census AS pc ON im.Insurance_Code = pc.PayerCode
	JOIN dbo.tbl_admissions AS a ON a.Admission_Date = pc.CensusDate
GROUP BY YEAR(Admission_Date), MONTH(Admission_Date)
ORDER BY Month

--SELECT * FROM dbo.tbl_admissions ORDER BY Admission_Date

--=========
--h. Find patients hospitalized more than once a month
--select * from tbl_patient_names;
WITH MonthlyAdmissions AS (
    SELECT DISTINCT
        pn.Patient_ID,
        pn.Name,
        YEAR(a.Admission_Date) AS Year,
        MONTH(a.Admission_Date) AS Month,
        COUNT(*) AS Admissions_Per_Month
    FROM dbo.tbl_admissions AS a
    JOIN dbo.tbl_patient_names AS pn ON pn.Patient_ID = a.Patient_ID
    GROUP BY pn.Patient_ID, pn.Name, YEAR(a.Admission_Date), MONTH(a.Admission_Date)
)
SELECT
    Patient_ID,
    REPLACE(Name, '@', '') AS Patient_Name,
    Year,
    Month,
    Admissions_Per_Month
FROM MonthlyAdmissions
WHERE Admissions_Per_Month > 1;


---------------------------
-- i.Aggregate the number of patients by hospital and insurance company in a month
-- Step 1: Check PatientDetails CTE
WITH PatientDetails AS (
    SELECT
        a.Patient_ID,
        a.Hospital_ID,
        hm.Hospital_Name,
        pc.PayerCode,
        im.Insurance_Company,
        a.Admission_Date
    FROM dbo.tbl_insurance_mapping AS im
    JOIN dbo.tbl_patient_census AS pc ON im.Insurance_Code = pc.PayerCode
    JOIN dbo.tbl_admissions AS a ON a.Admission_Date = pc.CensusDate
    JOIN dbo.tbl_hopsital_mapping AS hm ON hm.Hospital_ID = a.Hospital_ID
)
SELECT
    im.Insurance_Company,
    YEAR(pd.Admission_Date) AS Year,
    MONTH(pd.Admission_Date) AS Month,
    COUNT(DISTINCT pd.Patient_ID) AS Patient_Count,
    COUNT(*) AS Total_Rows,
    pd.Hospital_Name
FROM PatientDetails AS pd
JOIN dbo.tbl_insurance_mapping AS im ON im.Insurance_Code = pd.PayerCode
GROUP BY 
    im.Insurance_Company,
    YEAR(pd.Admission_Date),
    MONTH(pd.Admission_Date),
    pd.Hospital_Name
ORDER BY 
    im.Insurance_Company,
    Year,
    Month,
    pd.Hospital_Name;

------------------------------------------------------------------------

--5a. Create a user-defined function to calculate time spent by a nurse in a month
--SELECT * FROM dbo.tbl_labor_hours
CREATE FUNCTION dbo.GetMonthlyHours (
    @NurseName NVARCHAR(100),  
    @Month INT,                
    @Year INT                  
)
RETURNS FLOAT
AS
BEGIN
    DECLARE @TotalHours FLOAT;

    SELECT @TotalHours = SUM(Hours)
    FROM dbo.tbl_labor_hours
    WHERE Name = @NurseName
    RETURN COALESCE(@TotalHours, 0);  -- Return 0 if no hours are found
END;


---=============================
--5b. Calculate total earnings for nurses

SELECT 
    l.Name AS Name,
    l.Hospital_ID,
    l.Position,
    CAST(SUM(DATEDIFF(MINUTE, TRY_CONVERT(DATETIME, l.InPunchTime, 120), TRY_CONVERT(DATETIME, l.OutPunchTime, 120)) / 60.0) AS int)  AS Total_Hours_Worked,
    w.Hourly_Wage____hr_ AS Hourly_Wage,
    CAST(SUM(DATEDIFF(MINUTE, TRY_CONVERT(DATETIME, l.InPunchTime, 120), TRY_CONVERT(DATETIME, l.OutPunchTime, 120)) / 60.0) * w.Hourly_Wage____hr_ AS INT) AS Total_Earnings
FROM dbo.tbl_labor_hours AS l
JOIN dbo.tbl_hourly_wages AS w
    ON l.Position = w.Postion
WHERE l.Position = 'REGISTERED NURSE'  
GROUP BY 
    l.Name,
    l.Hospital_ID,
    l.Position,
    w.Hourly_Wage____hr_;

---=====================================================

--5c. Create a view that ranks hospitals based on nurse wages
CREATE VIEW RankedHospitalsByNurseWages AS
WITH HospitalWages AS (
    SELECT 
        l.Hospital_ID,
        h.Hospital_Name,
        SUM(l.Hours * w.Hourly_Wage____hr_) AS Total_Wages
    FROM dbo.tbl_labor_hours AS l
    JOIN dbo.tbl_hourly_wages AS w ON l.Position = w.Postion
    JOIN dbo.tbl_hopsital_mapping AS h ON l.Hospital_ID = h.Hospital_ID
    WHERE l.Position = 'NURSE'
    GROUP BY l.Hospital_ID, h.Hospital_Name
),
RankedHospitals AS (
    SELECT
        Hospital_ID,
        Hospital_Name,
        Total_Wages,
        RANK() OVER (ORDER BY Total_Wages DESC) AS Rank
    FROM HospitalWages
)
SELECT 
    Hospital_ID,
    Hospital_Name,
    Total_Wages,
    Rank
FROM RankedHospitals;


--5.d Create stored procedure to find the longest working streak for each nurse
CREATE PROCEDURE FindLongestWorkingStreak
AS
BEGIN
    WITH WorkingDays AS (
        SELECT
            l.Name AS Nurse_Name,
            CAST(l.InPunchTime AS DATE) AS Work_Date  -- Assuming you need to convert to just the date
        FROM dbo.tbl_labor_hours AS l
        WHERE l.Position = 'REGISTERED NURSE'
        GROUP BY l.Name, CAST(l.InPunchTime AS DATE)
    ),
    StreaksWithGaps AS (
        SELECT
            Nurse_Name,
            Work_Date,
            ROW_NUMBER() OVER (PARTITION BY Nurse_Name ORDER BY Work_Date) AS rn,
            DATEADD(DAY, -ROW_NUMBER() OVER (PARTITION BY Nurse_Name ORDER BY Work_Date), Work_Date) AS GroupingKey
        FROM WorkingDays
    ),
    StreakDurations AS (
        SELECT
            Nurse_Name,
            MIN(Work_Date) AS Streak_Start,
            MAX(Work_Date) AS Streak_End,
            DATEDIFF(DAY, MIN(Work_Date), MAX(Work_Date)) + 1 AS Streak_Duration
        FROM StreaksWithGaps
        GROUP BY Nurse_Name, GroupingKey
    )
    SELECT
        Nurse_Name,
        MAX(Streak_Duration) AS Longest_Streak_Days
    FROM StreakDurations
    GROUP BY Nurse_Name
    ORDER BY Nurse_Name;
END;
