import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
    host="dev-db.c71a1etj7mr8.ap-south-1.rds.amazonaws.com",
    port="3306",
    user="tasmacSales",
    password="bontonTasmac",
    database="tasmac_sales_data"
)
mycursor = mydb.cursor()

# Queries
queries = [
    """WITH current_year_sales AS (
    SELECT 
        SRM_id,
        DM_Office_id,
        SUM(imfs_sold_volume_cases) AS CURRENT_IMFS, 
        SUM(beer_sold_volume_cases) AS CURRENT_BEER,
        SUM(total_sales_value) AS CURRENT_SALES
    FROM 
        daily_district_wise_sales
    WHERE 
        Date BETWEEN 1 AND 7
    AND Month = 6
    AND Year = 2024
    GROUP BY
        SRM_id,
        DM_Office_id
),
previous_year_sales AS (
    SELECT 
        SRM_id,
        DM_Office_id,
        SUM(imfs_sold_volume_cases) AS LAST_IMFS, 
        SUM(beer_sold_volume_cases) AS LAST_BEER,
        SUM(total_sales_value) AS LAST_SALES
    FROM 
        daily_district_wise_sales
    WHERE 
        Date BETWEEN 1 AND 7
    AND Month = 6
    AND Year = 2023
    GROUP BY
        SRM_id,
        DM_Office_id
)
SELECT 
    b.SRM_Name,
    a.DM_office_name,
    c.SRM_id,
    c.DM_Office_id,
    c.CURRENT_IMFS,
    c.CURRENT_BEER,
    c.CURRENT_SALES,
    p.LAST_IMFS,
    p.LAST_BEER,
    p.LAST_SALES
FROM 
    current_year_sales c
JOIN 
    district_manager_offices a
ON 
    a.DM_id = c.DM_Office_id 
JOIN 
    srm_master b
ON 
    b.SRM_id = c.SRM_id
LEFT JOIN 
    previous_year_sales p
ON 
    c.SRM_id = p.SRM_id
AND 
    c.DM_Office_id = p.DM_Office_id;""",    
    
    """WITH current_year_sales AS (
    SELECT 
        SRM_id,
        DM_id,
        SUM(IMFS) AS CURRENT_IMFS, 
        SUM(BEER) AS CURRENT_BEER,
        SUM(TOTAL_SALES) AS CURRENT_SALES
    FROM 
        tasmac_sales_data.FL2_FL3_SUM
    WHERE 
        Date BETWEEN '2024-07-01' AND '2024-07-08'
    GROUP BY
        SRM_id,
        DM_id
),
previous_year_sales AS (
    SELECT 
        SRM_id,
        DM_id,
        SUM(IMFS) AS LAST_IMFS, 
        SUM(BEER) AS LAST_BEER,
        SUM(TOTAL_SALES) AS LAST_SALES
    FROM 
        tasmac_sales_data.FL2_FL3_SUM
    WHERE 
        Date BETWEEN '2023-07-01' AND '2023-07-08'
        
    GROUP BY
        SRM_id,
        DM_id
)
SELECT 
    b.SRM_Name,
    a.DM_office_name,
    p.SRM_id,
    p.DM_id,
    c.CURRENT_IMFS,
    c.CURRENT_BEER,
    c.CURRENT_SALES,
    p.LAST_IMFS,
    p.LAST_BEER,
    p.LAST_SALES
FROM 
    current_year_sales c
cross JOIN 
    district_manager_offices a
ON 
    a.DM_id = c.DM_id  
cross JOIN 
    srm_master b
ON 
    b.SRM_id = c.SRM_id
right JOIN 
    previous_year_sales p
ON 
    c.SRM_id = p.SRM_id
AND 
    c.DM_id = p.DM_id;"""
]

dfs = []
for query in queries:
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    column_names = [i[0] for i in mycursor.description]
    df = pd.DataFrame(myresult, columns=column_names)
    dfs.append(df)
print(dfs[0])
print(dfs[1])
# Ensure correct data types
for df in dfs:
    df['CURRENT_IMFS'] = df['CURRENT_IMFS'].astype(float)####current imfs cases %
    df['CURRENT_BEER'] = df['CURRENT_BEER'].astype(float)
    df['CURRENT_SALES'] = df['CURRENT_SALES'].apply(float)
    df['LAST_IMFS'] = df['LAST_IMFS'].astype(float)
    df['LAST_BEER'] = df['LAST_BEER'].astype(float)    
    df['LAST_SALES'] = df['LAST_SALES'].astype(float)
   

dict_data = [df.to_dict(orient='records') for df in dfs]


# Combine the data
combined_data = {}
for data in dict_data[0]:
    dm_office_name = data['DM_office_name']
    combined_data[dm_office_name] = data

for data in dict_data[1]:
    dm_office_name = data['DM_office_name']
    if dm_office_name in combined_data:
        combined_data[dm_office_name]['CURRENT_IMFS'] += data['CURRENT_IMFS']
        combined_data[dm_office_name]['CURRENT_BEER'] += data['CURRENT_BEER']
        combined_data[dm_office_name]['CURRENT_SALES'] += data['CURRENT_SALES']
        combined_data[dm_office_name]['LAST_IMFS'] += data['LAST_IMFS']
        combined_data[dm_office_name]['LAST_BEER'] += data['LAST_BEER']
        combined_data[dm_office_name]['LAST_SALES'] += data['LAST_SALES']        
    else:
        combined_data[dm_office_name] = data
df_combined = pd.DataFrame(combined_data.values())
df_combined.to_excel("output_vivid.xlsx", index=False)