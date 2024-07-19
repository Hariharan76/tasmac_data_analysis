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
    """SELECT 
        srm_master.SRM_Name,
        district_manager_offices.DM_office_name,    
        a.imfs_sold_volume_cases AS CURRENT_IMFS_CASES, 
        a.beer_sold_volume_cases AS CURRENT_BEER_CASES,
        a.total_sales_value / 10000000 AS CURRENT_Total_Sales_Cr,
        b.imfs_sold_volume_cases AS PREV_YEAR_IMFS_CASES, 
        b.beer_sold_volume_cases AS PREV_YEAR_BEER_CASES,
        b.total_sales_value / 10000000 AS PREV_YEAR_Total_Sales_Cr,
        c.imfs_sold_volume_cases AS LAST_WEEK_IMFS_CASES, 
        c.beer_sold_volume_cases AS LAST_WEEK_BEER_CASES,
        c.total_sales_value / 10000000 AS LAST_WEEK_Total_Sales_Cr
    FROM 
        daily_district_wise_sales a
        JOIN district_manager_offices ON district_manager_offices.DM_id = a.DM_Office_id
        JOIN srm_master ON srm_master.SRM_id = a.SRM_id
        JOIN daily_district_wise_sales b ON a.SRM_id = b.SRM_id AND a.DM_Office_id = b.DM_Office_id
        JOIN daily_district_wise_sales c ON a.SRM_id = c.SRM_id AND a.DM_Office_id = c.DM_Office_id
    WHERE 
        a.date = '7' AND 
        a.month = '7' AND 
        a.year = '2024' AND
        b.date = '7' AND 
        b.month = '7' AND 
        b.year = '2023' AND
        c.date = '30' AND 
        c.month = '6' AND 
        c.year = '2024';""",    
    """SELECT 
    a.SRM_Name,
    a.DM_office_name, 
    b.date,
    COALESCE(b.IMFS, 0) AS CURRENT_IMFS_CASES,
    COALESCE(b.BEER, 0) AS CURRENT_BEER_CASES,
    COALESCE(b.TOTAL_SALES, 0) AS CURRENT_Total_Sales_Cr,
    COALESCE(c.IMFS, 0) AS PREV_YEAR_IMFS_CASES,
    COALESCE(c.BEER, 0) AS PREV_YEAR_BEER_CASES,
    COALESCE(c.TOTAL_SALES, 0) AS PREV_YEAR_Total_Sales_Cr,
    COALESCE(d.IMFS, 0) AS LAST_WEEK_IMFS_CASES,
    COALESCE(d.BEER, 0) AS LAST_WEEK_BEER_CASES,
    COALESCE(d.TOTAL_SALES, 0) AS LAST_WEEK_Total_Sales_Cr
FROM 
    OFF_NAME a
LEFT JOIN 
    FL2_FL3_SUM b ON a.DM_id = b.DM_id AND a.SRM_id = b.SRM_id AND b.date = '2024-07-07'
LEFT JOIN 
    FL2_FL3_SUM c ON a.DM_id = c.DM_id AND a.SRM_id = c.SRM_id AND c.date = '2023-07-07'
LEFT JOIN 
    FL2_FL3_SUM d ON a.DM_id = d.DM_id AND a.SRM_id = d.SRM_id AND d.date = '2024-06-30';"""
]

dfs = []
for query in queries:
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    column_names = [i[0] for i in mycursor.description]
    df = pd.DataFrame(myresult, columns=column_names)
    dfs.append(df)
# print(dfs[0])
print(dfs[1])
# Ensure correct data types
for df in dfs:
    df['CURRENT_IMFS_CASES'] = df['CURRENT_IMFS_CASES'].astype(float)####current imfs cases %
    df['CURRENT_BEER_CASES'] = df['CURRENT_BEER_CASES'].astype(float)
    df['CURRENT_Total_Sales_Cr'] = df['CURRENT_Total_Sales_Cr'].apply(float)
    df['LAST_WEEK_IMFS_CASES'] = df['LAST_WEEK_IMFS_CASES'].astype(float)
    df['LAST_WEEK_BEER_CASES'] = df['LAST_WEEK_BEER_CASES'].astype(float)
    df['LAST_WEEK_Total_Sales_Cr'] = df['LAST_WEEK_Total_Sales_Cr'].apply(float)
    df["PREV_YEAR_Total_Sales_Cr"]=df["PREV_YEAR_Total_Sales_Cr"].apply(float)
    df["PREV_YEAR_IMFS_CASES"]=df["PREV_YEAR_IMFS_CASES"].astype(float)###current imfs cases %
    df["PREV_YEAR_BEER_CASES"]=df["PREV_YEAR_BEER_CASES"].astype(float)

dict_data = [df.to_dict(orient='records') for df in dfs]


# Combine the data
combined_data = {}
for data in dict_data[0]:
    dm_office_name = data['DM_office_name']
    combined_data[dm_office_name] = data

for data in dict_data[1]:
    dm_office_name = data['DM_office_name']
    if dm_office_name in combined_data:
        combined_data[dm_office_name]['CURRENT_IMFS_CASES'] += data['CURRENT_IMFS_CASES']
        combined_data[dm_office_name]['CURRENT_BEER_CASES'] += data['CURRENT_BEER_CASES']
        combined_data[dm_office_name]['CURRENT_Total_Sales_Cr'] += data['CURRENT_Total_Sales_Cr']
        combined_data[dm_office_name]['PREV_YEAR_IMFS_CASES'] += data['PREV_YEAR_IMFS_CASES']
        combined_data[dm_office_name]['PREV_YEAR_BEER_CASES'] += data['PREV_YEAR_BEER_CASES']
        combined_data[dm_office_name]['PREV_YEAR_Total_Sales_Cr'] += data['PREV_YEAR_Total_Sales_Cr']
        combined_data[dm_office_name]['LAST_WEEK_IMFS_CASES'] += data['LAST_WEEK_IMFS_CASES']
        combined_data[dm_office_name]['LAST_WEEK_BEER_CASES'] += data['LAST_WEEK_BEER_CASES']
        combined_data[dm_office_name]['LAST_WEEK_Total_Sales_Cr'] += data['LAST_WEEK_Total_Sales_Cr']
    else:
        combined_data[dm_office_name] = data
        

df_combined = pd.DataFrame(combined_data.values())
df_combined.to_excel("output2.xlsx", index=False)

