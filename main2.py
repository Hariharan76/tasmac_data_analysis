import mysql.connector
import pandas as pd
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI,Request, Form
from datetime import datetime as d
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
app = FastAPI()
templates = Jinja2Templates(directory="template")

mydb = mysql.connector.connect(
    host="dev-db.c71a1etj7mr8.ap-south-1.rds.amazonaws.com",
    port="3306",
    user="tasmacSales",
    password="bontonTasmac",
    database="tasmac_sales_data"
)
mycursor = mydb.cursor()
def get_largest_values(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nlargest(10, column)
    return largest.to_dict(orient='records')
def get_smallest_values(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nsmallest(10, column)
    return largest.to_dict(orient='records')

def work1(df):
    data_imfs=pd.DataFrame()
    data_beer=pd.DataFrame()
    data_sales=pd.DataFrame()
    data_imfs[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]
    data_beer[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]
    data_sales[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","SALES_Current_Year_Sales_percentage","SALES_Total_2022","SALES_Total_2021","SALES_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","Sales_Current_Year_Sales_percentage","SALES_Total_2022","SALES_Total_2021","Sales_Last_Year_Sales_percentage"]]   
    out={}
    data_imfs["current_year_percentage"]=pd.to_numeric(df["IMFS_Current_Year_Sales_percentage"])
    a=data_imfs.nlargest(10, 'current_year_percentage')
    b=data_imfs.nsmallest(10, 'current_year_percentage')
    out.update({"IMFS_largest":a.to_dict(orient='records'),"IMFS_smallest":b.to_dict(orient='records')})
    data_beer["current_year_beer_percentage"]=pd.to_numeric(df["BEER_Current_Year_Sales_percentage"])
    a=data_beer.nlargest(10, 'current_year_beer_percentage')
    b=data_beer.nsmallest(10, 'current_year_beer_percentage')
    out.update({"BEER_largest":a.to_dict(orient='records'),"BEER_smallest":b.to_dict(orient='records')})
    data_sales["current_year_sales_percentage"]=pd.to_numeric(df["Sales_Current_Year_Sales_percentage"])
    a=data_sales.nlargest(10, 'current_year_sales_percentage')
    b=data_sales.nsmallest(10, 'current_year_sales_percentage')
    out.update({"SALES_largest":a.to_dict(orient='records'),"SALES_smallest":b.to_dict(orient='records')})
    return out

def calculate_percentage_change(current, previous):
    if previous == 0:
        return 0 
    return ((current - previous) / previous) * 100



@app.post("/")
async def generate_report(date:int,district:str):
    selected_date = date
    queries = f"""SELECT district_name,
        shop_no,
        SUM(CASE WHEN YEAR(date) = 2024 THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS SALES_Total_2024,
        SUM(CASE WHEN YEAR(date) = 2023 THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS SALES_Total_2023,
        SUM(CASE WHEN YEAR(date) = 2022 THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS SALES_Total_2022,
        SUM(CASE WHEN YEAR(date) = 2021 THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS SALES_Total_2021,
        SUM(CASE WHEN YEAR(date) = 2024 THEN imfs_1000_ml_bottles + imfs_750_ml_bottles + imfs_375_ml_bottles + imfs_180_ml_bottles ELSE 0 END) AS IMFS_Total_2024,
        SUM(CASE WHEN YEAR(date) = 2023 THEN imfs_1000_ml_bottles + imfs_750_ml_bottles + imfs_375_ml_bottles + imfs_180_ml_bottles ELSE 0 END) AS IMFS_Total_2023,
        SUM(CASE WHEN YEAR(date) = 2022 THEN imfs_1000_ml_bottles + imfs_750_ml_bottles + imfs_375_ml_bottles + imfs_180_ml_bottles ELSE 0 END) AS IMFS_Total_2022,
        SUM(CASE WHEN YEAR(date) = 2021 THEN imfs_1000_ml_bottles + imfs_750_ml_bottles + imfs_375_ml_bottles + imfs_180_ml_bottles ELSE 0 END) AS IMFS_Total_2021,
        SUM(CASE WHEN YEAR(date) = 2024 THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Total_2024,
        SUM(CASE WHEN YEAR(date) = 2023 THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Total_2023,
        SUM(CASE WHEN YEAR(date) = 2022 THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Total_2022,
        SUM(CASE WHEN YEAR(date) = 2021 THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Total_2021
    FROM
        RV_SALES_DAILY
    WHERE
        MONTH(date) = 7
        AND DAY(date) BETWEEN 1 AND {selected_date}
        AND YEAR(date) IN (2021, 2022, 2023, 2024)
    GROUP BY
        shop_no, district_name
    HAVING
        SUM(CASE WHEN YEAR(date) = 2024 THEN imfs_sales_value + beer_sales_value ELSE 0 END) <> 0
    ORDER BY
        district_name asc;"""

    mycursor.execute(queries)
    myresult = mycursor.fetchall()
    column_names = [i[0] for i in mycursor.description]
    df = pd.DataFrame(myresult, columns=column_names)
    ########### sales percentages
    df["Sales_Current_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2024"], row["SALES_Total_2023"]),axis=1)
    df['Sales_Current_Year_Sales_percentage'] = pd.to_numeric(df['Sales_Current_Year_Sales_percentage'])
    df['Sales_Current_Year_Sales_percentage'] = df['Sales_Current_Year_Sales_percentage'].round(1)

    df["Sales_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2022"], row["SALES_Total_2021"]),axis=1)
    df['Sales_Last_Year_Sales_percentage'] = pd.to_numeric(df['Sales_Last_Year_Sales_percentage'])
    df['Sales_Last_Year_Sales_percentage'] = df['Sales_Last_Year_Sales_percentage'].round(1)
    ##############IMS Percentages
    df["IMFS_Current_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["IMFS_Total_2024"], row["IMFS_Total_2023"]),axis=1)
    df['IMFS_Current_Year_Sales_percentage'] = pd.to_numeric(df['IMFS_Current_Year_Sales_percentage'])
    df['IMFS_Current_Year_Sales_percentage'] = df['IMFS_Current_Year_Sales_percentage'].round(1)

    df["IMFS_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2022"], row["SALES_Total_2021"]),axis=1)
    df['IMFS_Last_Year_Sales_percentage'] = pd.to_numeric(df['IMFS_Last_Year_Sales_percentage'])
    df['IMFS_Last_Year_Sales_percentage'] = df['IMFS_Last_Year_Sales_percentage'].round(1)
    ################## BEER Percentage
    df["BEER_Current_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["BEER_Total_2024"], row["BEER_Total_2023"]),axis=1)
    df['BEER_Current_Year_Sales_percentage'] = pd.to_numeric(df['BEER_Current_Year_Sales_percentage'])
    df['BEER_Current_Year_Sales_percentage'] = df['BEER_Current_Year_Sales_percentage'].round(1)
    df["BEER_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["BEER_Total_2022"], row["BEER_Total_2021"]),axis=1)
    df['BEER_Last_Year_Sales_percentage'] = pd.to_numeric(df['BEER_Last_Year_Sales_percentage'])
    df['BEER_Last_Year_Sales_percentage'] = df['BEER_Last_Year_Sales_percentage'].round(1)
    if district=="ALL":
        data=work1(df)
        return data
    else:
        data={}
        district_name=district
        data_imfs=pd.DataFrame()
        data_beer=pd.DataFrame()
        data_sales=pd.DataFrame()
        data_imfs[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]
        data_beer[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]
        data_sales[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","SALES_Current_Year_Sales_percentage","SALES_Total_2022","SALES_Total_2021","SALES_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","Sales_Current_Year_Sales_percentage","SALES_Total_2022","SALES_Total_2021","Sales_Last_Year_Sales_percentage"]]
        imfs_largest = get_largest_values(data_imfs, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_largest = get_largest_values(data_beer, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_largest = get_largest_values(data_sales, district_name, 'SALES_Current_Year_Sales_percentage')
        imfs_smallest = get_smallest_values(data_imfs, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_smallest = get_smallest_values(data_beer, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_smalllest = get_smallest_values(data_sales, district_name, 'SALES_Current_Year_Sales_percentage')
        data.update({"IMFS_largest":imfs_largest,"IMFS_smallest":imfs_smallest,"BEER_largest":beer_largest,"BEER_smallest":beer_smallest,"SALES_largest":sales_largest,"SALES_smallest":sales_smalllest})
        return data






