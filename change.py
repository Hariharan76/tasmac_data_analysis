import mysql.connector
import pandas as pd

# Connect to the database
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
    b.imfs_sold_volume_cases AS PREV_DAY_IMFS_CASES, 
    b.beer_sold_volume_cases AS PREV_DAY_BEER_CASES,
    b.total_sales_value / 10000000 AS PREV_DAY_Total_Sales_Cr,
    c.imfs_sold_volume_cases AS LAST_WEEK_IMFS_CASES, 
    c.beer_sold_volume_cases AS LAST_WEEK_BEER_CASES,
    c.total_sales_value / 10000000 AS LAST_WEEK_Total_Sales_Cr
FROM 
    daily_district_wise_sales a
    join 
    district_manager_offices
    on district_manager_offices.DM_id= a.DM_Office_id
    join 
    srm_master
    on srm_master.SRM_id= a.SRM_id
JOIN 
    daily_district_wise_sales b
ON 
    a.SRM_id = b.SRM_id AND a.DM_Office_id = b.DM_Office_id
JOIN 
    daily_district_wise_sales c
ON 
    a.SRM_id = c.SRM_id AND a.DM_Office_id = c.DM_Office_id
WHERE 
    a.date = '3' AND 
    a.month = '7' AND 
    a.year = '2024' AND
    b.date = '3' AND 
    b.month = '7' AND 
    b.year = '2024' AND
    c.date = '26' AND 
    c.month = '6' AND 
    c.year = '2024';""",
    """SELECT 
         srm_master.SRM_Name,
         district_manager_offices.DM_office_name,   
        a.FL2_FL3_IMFS_volume_sold AS CURRENT_IMFS_CASES, 
        a.FL2_FL3_Beer_volume_sold AS CURRENT_BEER_CASES,
        a.FL2_FL3_Total_Sales_Values / 10000000 AS CURRENT_Total_Sales_Cr,
        b.FL2_FL3_IMFS_volume_sold AS PREV_DAY_IMFS_CASES, 
        b.FL2_FL3_Beer_volume_sold AS PREV_DAY_BEER_CASES,
        b.FL2_FL3_Total_Sales_Values / 10000000 AS PREV_DAY_Total_Sales_Cr,
        c.FL2_FL3_IMFS_volume_sold AS LAST_WEEK_IMFS_CASES, 
        c.FL2_FL3_Beer_volume_sold AS LAST_WEEK_BEER_CASES,
        c.FL2_FL3_Total_Sales_Values / 10000000 AS LAST_WEEK_Total_Sales_Cr
    FROM 
        FL2_FL3_DAILY_SALES a
        join district_manager_offices
        on district_manager_offices.DM_id=a.DM_id

        join srm_master
        on srm_master.SRM_id=a.SRM_id
    JOIN 
        FL2_FL3_DAILY_SALES b ON a.SRM_id = b.SRM_id AND a.DM_id = b.DM_id
    JOIN 
        FL2_FL3_DAILY_SALES c ON a.SRM_id = c.SRM_id AND a.DM_id = c.DM_id
    WHERE 
        a.date = '2024-07-03' 
        AND b.date = '2023-07-03' 
        AND c.date = '2024-06-26';"""
]

# Execute queries and store results
dfs = []
for query in queries:
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    column_names = [i[0] for i in mycursor.description]
    df = pd.DataFrame(myresult, columns=column_names)
    dfs.append(df)

    

for df in dfs:
    df['CURRENT_IMFS_CASES'] = df['CURRENT_IMFS_CASES'].astype(float)
    df['CURRENT_BEER_CASES'] = df['CURRENT_BEER_CASES'].astype(float)
    df['CURRENT_Total_Sales_Cr'] = df['CURRENT_Total_Sales_Cr'].apply(float)
    df['LAST_WEEK_IMFS_CASES'] = df['LAST_WEEK_IMFS_CASES'].astype(float)
    df['LAST_WEEK_BEER_CASES'] = df['LAST_WEEK_BEER_CASES'].astype(float)
    df['LAST_WEEK_Total_Sales_Cr'] = df['LAST_WEEK_Total_Sales_Cr'].apply(float)

    

# Sum the corresponding columns
combined_df = pd.DataFrame()
for i in dfs[0]["DM_office_name"]:
    for j in dfs[1]["DM_office_name"]:
        if i==j:
            combined_df["SRM_Name"]=dfs[0]["SRM_Name"]
            combined_df["DM_office_name"]=dfs[0]["DM_office_name"]
            combined_df['CURRENT_IMFS_CASES'] = dfs[0]['CURRENT_IMFS_CASES'] + dfs[1]['CURRENT_IMFS_CASES']
            combined_df['CURRENT_BEER_CASES'] = dfs[0]['CURRENT_BEER_CASES'] + dfs[1]['CURRENT_BEER_CASES']
            combined_df['CURRENT_Total_Sales_Cr'] = dfs[0]['CURRENT_Total_Sales_Cr'] + dfs[1]['CURRENT_Total_Sales_Cr']
            combined_df["LAST_WEEK_IMFS_CASES"] = dfs[0]["LAST_WEEK_IMFS_CASES"] + dfs[1]["LAST_WEEK_IMFS_CASES"]
            combined_df["LAST_WEEK_BEER_CASES"] = dfs[0]["LAST_WEEK_BEER_CASES"] + dfs[1]["LAST_WEEK_BEER_CASES"]
            combined_df["LAST_WEEK_Total_Sales_Cr"] = dfs[0]["LAST_WEEK_Total_Sales_Cr"] + dfs[1]["LAST_WEEK_Total_Sales_Cr"]

    # Export the combined DataFrame to an Excel file
    combined_df.to_excel("combined_sales_data.xlsx", index=False)

    print("Data has been exported to combined_sales_data.xlsx")
