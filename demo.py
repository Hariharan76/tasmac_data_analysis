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
Quary=["""SELECT 
    a.SRM_id,
    a.DM_Office_id,
    a.imfs_sold_volume_cases AS CURRENT_IMFS_CASES, 
    a.beer_sold_volume_cases AS CURRENT_BEER_CASES,
    a.total_sales_value / 10000000 AS CURRENT_Total_Sales_Cr,
       b.imfs_sold_volume_cases AS LAST_IMFS_CASES, 
    b.beer_sold_volume_cases AS LAST_BEER_CASES,
    b.total_sales_value / 10000000 AS LAST_Total_Sales_Cr
FROM 
    daily_district_wise_sales a
JOIN 
    daily_district_wise_sales b
ON 
    a.SRM_id = b.SRM_id AND a.DM_Office_id = b.DM_Office_id
WHERE 
    a.date = '4' AND 
    a.month = '7' AND 
    a.year = '2024' AND
    b.date = '4' AND 
    b.month = '7' AND 
    b.year = '2024';""","""SELECT 
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
    AND c.date = '2024-06-26';"""]
for i in Quary:
    query =i 
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    column_names = [i[0] for i in mycursor.description]
    df = pd.DataFrame(myresult, columns=column_names)
    json_result = df.to_json(orient="records")
    print(json_result)