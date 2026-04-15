CREATE DATABASE ManufacturingDB;
USE ManufacturingDB;
CREATE TABLE machine_quality_data (
Timestamp DATETIME,
    MachineID INT,
    Plant VARCHAR(50),
    Temperature FLOAT,
    Vibration FLOAT,
    Pressure FLOAT,
    EnergyConsumption FLOAT,
    ProductionUnits INT,
    DefectCount INT,
    MaintenanceFlag INT
);
SELECT * FROM machine_quality_data LIMIT 10;
SELECT COUNT(*) FROM machine_quality_data;

SELECT * 
FROM machine_quality_data
WHERE ProductionUnits IS NULL OR DefectCount IS NULL;

WITH daily_data AS (
    SELECT 
        DATE(Timestamp) AS Date,
        Plant,
        SUM(DefectCount) AS Total_Defects,
        SUM(ProductionUnits) AS Total_Units,
        (SUM(DefectCount) * 100.0 / SUM(ProductionUnits)) AS DefectRate
    FROM machine_quality_data
    GROUP BY DATE(Timestamp), Plant
)

SELECT * 
FROM daily_data
ORDER BY Plant, Date;

WITH daily_data AS (
    SELECT 
        DATE(Timestamp) AS Date,
        Plant,
        (SUM(DefectCount) * 100.0 / SUM(ProductionUnits)) AS DefectRate
    FROM machine_quality_data
    GROUP BY DATE(Timestamp), Plant
)

SELECT 
    Date,
    Plant,
    DefectRate,

    -- Rolling 7-day average
    AVG(DefectRate) OVER (
        PARTITION BY Plant
        ORDER BY Date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS Rolling_7Day_Avg_DefectRate

FROM daily_data
ORDER BY Plant, Date;

-- Machine wise average
SELECT 
    MachineID,
    Plant,
    AVG(DefectCount * 100.0 / ProductionUnits) AS Avg_DefectRate,
    AVG(Temperature) AS Avg_Temperature,
    AVG(Vibration) AS Avg_Vibration,
    AVG(Pressure) AS Avg_Pressure
FROM machine_quality_data
GROUP BY MachineID, Plant;

-- Top machines
SELECT 
    MachineID,
    Plant,
    AVG(DefectCount * 100.0 / ProductionUnits) AS Avg_DefectRate,
    AVG(Temperature) AS Avg_Temperature,
    AVG(Vibration) AS Avg_Vibration,
    AVG(Pressure) AS Avg_Pressure,
    PERCENT_RANK() OVER (
        PARTITION BY Plant
        ORDER BY AVG(DefectCount * 100.0 / ProductionUnits) DESC
    ) AS rank_percent
FROM machine_quality_data
GROUP BY MachineID, Plant;

-- Filter top 10 machines
SELECT *
FROM (
    SELECT 
        MachineID,
        Plant,
        AVG(DefectCount * 100.0 / ProductionUnits) AS Avg_DefectRate,
        AVG(Temperature) AS Avg_Temperature,
        AVG(Vibration) AS Avg_Vibration,
        AVG(Pressure) AS Avg_Pressure,

        PERCENT_RANK() OVER (
            PARTITION BY Plant
            ORDER BY AVG(DefectCount * 100.0 / ProductionUnits) DESC
        ) AS rank_percent

    FROM machine_quality_data
    GROUP BY MachineID, Plant
) ranked_data
WHERE rank_percent <= 0.10;

-- create the view
CREATE VIEW quality_hotspots_2025 AS
SELECT *
FROM (
    SELECT 
        MachineID,
        Plant,
        AVG(DefectCount * 100.0 / ProductionUnits) AS Avg_DefectRate,
        AVG(Temperature) AS Avg_Temperature,
        AVG(Vibration) AS Avg_Vibration,
        AVG(Pressure) AS Avg_Pressure,

        PERCENT_RANK() OVER (
            PARTITION BY Plant
            ORDER BY AVG(DefectCount * 100.0 / ProductionUnits) DESC
        ) AS rank_percent

    FROM machine_quality_data
    GROUP BY MachineID, Plant
) ranked_data
WHERE rank_percent <= 0.10;

SELECT * FROM quality_hotspots_2025;

