import duckdb as ddb
import os
import subprocess
import platform

#Automatically opens a given file for any OS
def open_file(filepath1):
    if platform.system() == "Windows":
        os.startfile(filepath1)

    elif platform.system() == "Darwin":  # macOS
        subprocess.run(["open", filepath1])

    else:  # Linux and others
        subprocess.run(["xdg-open", filepath1])

#Takes in the WellProduction file and the Wells dataset and generates a new dataset only containing rows with
#Avg well production 10 or more barrels and a summary dataset containing information for each company.
def GenerateFiles(production, wells):
    con = ddb.connect()
    #Calculate each wells total production, days of production, and the years avg production
    #Join with Wells dataset
    query = f"""
    WITH consolidated AS 
        (SELECT APINumber, 
            SUM(OilorCondensateProduced) AS TotalProduction, 
            SUM(DaysProducing) AS TotalDaysOfProduction, 
            CASE 
                WHEN SUM(DaysProducing) = 0 THEN 0
                ELSE (SUM(OilorCondensateProduced) / SUM(DaysProducing))
            END AS YearlyAVGWells
        FROM read_csv_auto('{production}')
        GROUP BY APINumber
        HAVING (YearlyAVGWells) >= 10
        ORDER BY APINumber)    
    SELECT 
        T2.API, 
        T2.OperatorName, 
        T2.Operatorcode,
        T1.TotalProduction, 
        T1.TotalDaysOfProduction,
        T1.YearlyAVGWells,
        T2.OperatorStatus,
        T2.FieldCode,
        T2.AreaCode,
        T2.PoolCode,
        T2.WellTypeCode,
        T2.LeaseName,
        T2.FieldName,
        T2.AreaName,
        T2.PoolName,
        T2.WellNumber,
        T2.WellStatus,
        T2.PoolWellTypeStatus,
        T2.County,
        T2.District,
        T2.Section,
        T2.SubSection,
        T2.Township,
        T2.Range,
        T2.BM,
        T2.SystemEntryDate
    FROM consolidated as T1
    JOIN read_csv_auto('{wells}') as T2 on T1.APINumber=T2.API
    ORDER BY T2.OperatorName, T1.YearlyAVGWells DESC
    """

    #Save dataset
    con.execute(f"""
    COPY (
        {query}
    ) TO 'dataset.csv' (HEADER, DELIMITER ',');
    """)

    #Calculate the highest yearly average for each company
    #Calculate the total production and days of production across all wells, and the number of wells each company has
    #Combine the data to show each companies total production, days of production, well API number with the highest
    # average production, the highest average production, the yearly average production across all wells,
    #and the total number of wells producing at least 10 barrels
    query = f"""
    WITH LargestWell AS (
        SELECT 
            OperatorName,
            MAX(YearlyAVGWells) AS HighestYearlyAVG
        FROM read_csv_auto('dataset.csv')
        GROUP BY OperatorName),
        TotalProductionData AS (
        SELECT 
            OperatorName,
            SUM(TotalProduction) AS TotalProduction,
            SUM(TotalDaysOfProduction) AS TotalDaysOfProduction,
            COUNT(OperatorName) AS TotalWellsProducing10orMore
        FROM read_csv_auto('dataset.csv')
        GROUP BY OperatorName)
    SELECT
        T1.OperatorName AS OperatorName,
        T3.TotalProduction AS TotalProduction,
        T3.TotalDaysOfProduction AS TotalDaysOfProduction,
        (SUM(T3.TotalProduction) / SUM(T3.TotalDaysOfProduction)) AS YearlyAVGAllWells,
        T2.API AS HighestProducingWellAPINumber,
        T1.HighestYearlyAVG AS HighestAVG,
        T3.TotalWellsProducing10orMore
    FROM LargestWell AS T1 
    JOIN read_csv_auto('dataset.csv') AS T2
    ON T1.OperatorName = T2.OperatorName
    JOIN TotalProductionData AS T3 on T1.OperatorName = T3.OperatorName
    GROUP BY T1.OperatorName, HighestProducingWellAPINumber, HighestAVG, YearlyAvgWells, T3.TotalProduction,
    T3.TotalDaysOfProduction, T3.TotalWellsProducing10orMore
    HAVING HighestAVG = T2.YearlyAVGWells
    ORDER BY T1.OperatorName
"""

    #generate summary file
    con.execute(f"""
    COPY (
        {query}
    ) TO 'summary.csv' (HEADER, DELIMITER ',');
    """)

    open_file('summary.csv')
    open_file('dataset.csv')

if __name__ == '__main__':
    GenerateFiles("2025CaliforniaOilAndGasWellMonthlyProduction.csv", "2025CaliforniaOilAndGasWells.csv")

