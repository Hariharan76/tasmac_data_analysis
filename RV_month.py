import mysql.connector
import pandas as pd
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI,Request, Form
from datetime import datetime as d
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from fastapi.staticfiles import StaticFiles
import matplotlib.pyplot as plt
import os
from pydantic import BaseModel
import matplotlib.pyplot as plt
import os
IMAGEDIR = "images/"

# app.mount("/static", StaticFiles(directory="static"), name="static")


app = FastAPI()
templates = Jinja2Templates(directory="template")
app.mount("/images", StaticFiles(directory=IMAGEDIR), name="images")
class Caputer_objet():
    Data={}

def demo(date):
    given_date = datetime.strptime(date, '%Y-%m-%d')
    last_year_same_month = given_date.replace(year=given_date.year - 1)
    current_month_last_month = given_date.replace(day=1) - timedelta(days=1)
    current_month_last_year = given_date.replace(year=given_date.year - 1)
    last_month_last_year = (given_date.replace(day=1) - timedelta(days=1)).replace(year=given_date.year - 1)
    current_month_two_months_ago = given_date - relativedelta(months=2)
    last_year_current_month_two_months_ago = current_month_two_months_ago.replace(year=given_date.year - 1)
    output_months = {
        "Given date": given_date.strftime('%b-%Y'),
        "Last year same month": last_year_same_month.strftime('%b-%Y'),
        "Current month last month": current_month_last_month.strftime('%b-%Y'),
        "Current month last year": current_month_last_year.strftime('%b-%Y'),
        "Last month last year": last_month_last_year.strftime('%b-%Y'),
        "Current month two months ago": current_month_two_months_ago.strftime('%b-%Y'),
        "Last year current month two months ago": last_year_current_month_two_months_ago.strftime('%b-%Y')
    }
    return output_months
mydb = mysql.connector.connect(
    host="dev-db.c71a1etj7mr8.ap-south-1.rds.amazonaws.com",
    port="3306",
    user="tasmacSales",
    password="bontonTasmac",
    database="tasmac_sales_data"
)
def calculate_percentage_change(current, previous):
    if previous == 0:
        return 0 
    return ((current - previous) / previous) * 100

mycursor = mydb.cursor()
def get_largest_values(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nlargest(10, column)
    return largest.to_dict(orient='records')
def get_smallest_values(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nsmallest(10, column)
    return largest.to_dict(orient='records')
def get_largest_values1(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nlargest(5, column)
    return largest
def get_smallest_values1(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nsmallest(5, column)
    return largest
def work1(df):
    data_imfs=pd.DataFrame()
    data_beer=pd.DataFrame()
    data_sales=pd.DataFrame()
    data_imfs[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_2023-2022_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_2023-2022_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]
    data_beer[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_2023_2022_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_2023_2022_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]
    data_sales[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","SALES_Current_Year_Sales_percentage","Sales_2023-2022_Sales_percentage","SALES_Total_2022","SALES_Total_2021","SALES_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","Sales_Current_Year_Sales_percentage","Sales_2023-2022_Sales_percentage","SALES_Total_2022","SALES_Total_2021","Sales_Last_Year_Sales_percentage"]]   
    Caputer_objet.Data.update({"imfs":data_imfs,"beer":data_beer,"sales":data_sales})
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

def work(df_combined):
    data_imfs=pd.DataFrame()
    data_beer=pd.DataFrame()
    data_sales=pd.DataFrame()
    data_imfs[["district_name","Shop no","current_month_sales","last_year_sales","current_month_percentage","Last_month_sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","IMFS_Current_Month_Sales","IMFS_Last_Year_Current_Month_Sales","IMFS_Current_Month_Sales_percentage","IMFS_Last_Month_Sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]
    data_beer[["district_name","Shop no","current_month_sales","last_year_sales","current_month_beer_percentage","Last_month_sales","Beer_Last_Year_Last_Month_Sales","beer_last_Month_Sales_percentage","beer_Two_Months_Ago_Sales","beer_Last_Year_Two_Months_Ago_Sales","beer_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","BEER_Current_Month_Sales","BEER_Last_Year_Current_Month_Sales","BEER_Current_Month_Sales_percentage","BEER_Last_Month_Sales","BEER_Last_Year_Last_Month_Sales","BEER_last_Month_Sales_percentage","BEER_Two_Months_Ago_Sales","BEER_Last_Year_Two_Months_Ago_Sales","BEER_Two_Month_ago_Sales_percentage"]]
    data_sales[["district_name","Shop no","current_month_sales","last_year_sales","current_month_sales_percentage","Last_month_sales","sales_Last_Year_Last_Month_Sales","sales_last_Month_Sales_percentage","sales_Two_Months_Ago_Sales","sales_Last_Year_Two_Months_Ago_Sales","sales_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","Sales_Current_Month_Sales","Sales_Last_Year_Current_Month_Sales","SALES_Current_Month_Sales_percentage","Sales_Last_Month_Sales","Sales_Last_Year_Last_Month_Sales","SALES_last_Month_Sales_percentage","Sales_Two_Months_Ago_Sales","Sales_Last_Year_Two_Months_Ago_Sales","SALES_Two_Month_ago_Sales_percentage"]]
    out={}
    data_imfs["current_month_percentage"]=pd.to_numeric(df_combined["IMFS_Current_Month_Sales_percentage"])
    a=data_imfs.nlargest(10, 'current_month_percentage')
    b=data_imfs.nsmallest(10, 'current_month_percentage')
    out.update({"IMFS_largest":a.to_dict(orient='records'),"IMFS_smallest":b.to_dict(orient='records')})
    data_beer["current_month_beer_percentage"]=pd.to_numeric(df_combined["BEER_Current_Month_Sales_percentage"])
    a=data_beer.nlargest(10, 'current_month_beer_percentage')
    b=data_beer.nsmallest(10, 'current_month_beer_percentage')
    out.update({"BEER_largest":a.to_dict(orient='records'),"BEER_smallest":b.to_dict(orient='records')})
    data_sales["current_month_sales_percentage"]=pd.to_numeric(df_combined["SALES_Current_Month_Sales_percentage"])
    a=data_sales.nlargest(10, 'current_month_sales_percentage')
    b=data_sales.nsmallest(10, 'current_month_sales_percentage')
    out.update({"SALES_largest":a.to_dict(orient='records'),"SALES_smallest":b.to_dict(orient='records')})
    return out

@app.get("/", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/month.html", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("month.html", {"request": request})

@app.get("/year.html", response_class=HTMLResponse)
async def read_form(request: Request):
    return templates.TemplateResponse("year.html", {"request": request})
    

@app.post("/report", response_class=HTMLResponse)
async def generate_report(request: Request, date: str = Form(...),district: str = Form(...)):
    selected_date = d.strptime(date, "%Y-%m-%d").day  
    data1=demo(date)  
    mycursor.execute(f"""WITH SalesTotals AS (
        SELECT
            shop_no,
            district_name,
            SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Two_Months_Ago_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(CURRENT_DATE) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Last_Year_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Last_Year_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN imfs_sales_value + beer_sales_value ELSE 0 END) AS Sales_Last_Year_Two_Months_Ago_Sales,           
            SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Two_Months_Ago_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(CURRENT_DATE) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Last_Year_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Last_Year_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN imfs_1000_ml_bottles + imfs_180_ml_bottles + imfs_375_ml_bottles + imfs_750_ml_bottles ELSE 0 END) AS IMFS_Last_Year_Two_Months_Ago_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Two_Months_Ago_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(CURRENT_DATE) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Last_Year_Current_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Last_Year_Last_Month_Sales,
            SUM(CASE WHEN YEAR(date) = YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)) AND MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)) THEN beer_650_ml_bottles + beer_325_plus_500_ml_bottles ELSE 0 END) AS BEER_Last_Year_Two_Months_Ago_Sales
        FROM
            RV_SALES_DAILY
        WHERE
            (MONTH(date) = MONTH(CURRENT_DATE) OR
            MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)) OR
            MONTH(date) = MONTH(DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH)))
            AND DAY(date) BETWEEN 1 AND {selected_date}
            AND YEAR(date) IN (YEAR(CURRENT_DATE), YEAR(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)))
        
        GROUP BY
            shop_no, district_name
    )
    SELECT
        district_name,
        shop_no,
        IMFS_Current_Month_Sales,
        IMFS_Last_Year_Current_Month_Sales,
        IMFS_Last_Month_Sales,
        IMFS_Last_Year_Last_Month_Sales,
        IMFS_Two_Months_Ago_Sales,
        IMFS_Last_Year_Two_Months_Ago_Sales,
        
        BEER_Current_Month_Sales,
        BEER_Last_Year_Current_Month_Sales,
        BEER_Last_Month_Sales,
        BEER_Last_Year_Last_Month_Sales,
        BEER_Two_Months_Ago_Sales,
        BEER_Last_Year_Two_Months_Ago_Sales,
        
        Sales_Current_Month_Sales,
        Sales_Last_Year_Current_Month_Sales,
        Sales_Last_Month_Sales,
        Sales_Last_Year_Last_Month_Sales,
        Sales_Two_Months_Ago_Sales,
        Sales_Last_Year_Two_Months_Ago_Sales
        
    FROM
        SalesTotals WHERE Sales_Current_Month_Sales <> 0""")
    
    myresult = mycursor.fetchall()    
    column_names = [desc[0] for desc in mycursor.description]
    df_combined = pd.DataFrame(myresult, columns=column_names)
        ############################### IMFS ########################################################################
    ###current Month IMFS
    df_combined["IMFS_Current_Month_Sales_percentage"]= df_combined.apply(lambda row: calculate_percentage_change(row["IMFS_Current_Month_Sales"], row["IMFS_Last_Year_Current_Month_Sales"]),axis=1)
    df_combined['IMFS_Current_Month_Sales_percentage'] = pd.to_numeric(df_combined['IMFS_Current_Month_Sales_percentage'])
    df_combined['IMFS_Current_Month_Sales_percentage'] = df_combined['IMFS_Current_Month_Sales_percentage'].round(1)
    ###### Last Month IMFS
    df_combined["IMFS_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['IMFS_Last_Month_Sales'] , row['IMFS_Last_Year_Last_Month_Sales'] ),axis=1)
    df_combined['IMFS_last_Month_Sales_percentage'] = pd.to_numeric(df_combined['IMFS_last_Month_Sales_percentage'])
    df_combined['IMFS_last_Month_Sales_percentage'] = df_combined['IMFS_last_Month_Sales_percentage'].round(1)
    ####### Two Month  AGO IMFS
    df_combined["IMFS_Two_Month_ago_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change(row['IMFS_Two_Months_Ago_Sales'] , row['IMFS_Last_Year_Two_Months_Ago_Sales']),axis=1) 
    df_combined['IMFS_Two_Month_ago_Sales_percentage'] = pd.to_numeric(df_combined['IMFS_Two_Month_ago_Sales_percentage'])
    df_combined['IMFS_Two_Month_ago_Sales_percentage'] = df_combined['IMFS_Two_Month_ago_Sales_percentage'].round(1)
    #################################################################################################################################################################################
    #################################### BEER #########################################################################################################################################
    ###current Month IMFS
    df_combined["BEER_Current_Month_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change (row['BEER_Current_Month_Sales'], row['BEER_Last_Year_Current_Month_Sales'] ),axis=1) 
    df_combined['BEER_Current_Month_Sales_percentage'] = pd.to_numeric(df_combined['BEER_Current_Month_Sales_percentage'])
    df_combined['BEER_Current_Month_Sales_percentage'] = df_combined['BEER_Current_Month_Sales_percentage'].round(1)
    ###### Last Month IMFS
    df_combined["BEER_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['BEER_Last_Month_Sales'] , row['BEER_Last_Year_Last_Month_Sales']),axis=1)
    df_combined['BEER_last_Month_Sales_percentage'] = pd.to_numeric(df_combined['BEER_last_Month_Sales_percentage'])     
    df_combined['BEER_last_Month_Sales_percentage'] = df_combined['BEER_last_Month_Sales_percentage'].round(1)
    ####### Two Month  AGO IMFS
    df_combined["BEER_Two_Month_ago_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['BEER_Two_Months_Ago_Sales'],row['BEER_Last_Year_Two_Months_Ago_Sales']),axis=1)
    df_combined['BEER_Two_Month_ago_Sales_percentage'] =pd.to_numeric(df_combined['BEER_Two_Month_ago_Sales_percentage'])
    df_combined['BEER_Two_Month_ago_Sales_percentage'] = df_combined['BEER_Two_Month_ago_Sales_percentage'].round(1)
    #########################################################################################################################################################################################
    ################################################################# SALES REPORT ####################################################################################################################
    df_combined["SALES_Current_Month_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Current_Month_Sales'] , row['Sales_Last_Year_Current_Month_Sales'] ),axis=1)
    df_combined['SALES_Current_Month_Sales_percentage'] =pd.to_numeric(df_combined['SALES_Current_Month_Sales_percentage'])
    df_combined['SALES_Current_Month_Sales_percentage'] = df_combined['SALES_Current_Month_Sales_percentage'].round(1)
    ###### Last Month IMFS
    df_combined["SALES_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Last_Month_Sales'],row['Sales_Last_Year_Last_Month_Sales']),axis=1 ) 
    df_combined['SALES_last_Month_Sales_percentage'] = pd.to_numeric(df_combined['SALES_last_Month_Sales_percentage'])
    df_combined['SALES_last_Month_Sales_percentage'] = df_combined['SALES_last_Month_Sales_percentage'].round(1)
    ####### Two Month  AGO IMFS
    df_combined["SALES_Two_Month_ago_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Two_Months_Ago_Sales'], row['Sales_Last_Year_Two_Months_Ago_Sales']),axis=1)
    df_combined['SALES_Two_Month_ago_Sales_percentage'] = pd.to_numeric(df_combined['SALES_Two_Month_ago_Sales_percentage'])
    df_combined['SALES_Two_Month_ago_Sales_percentage'] = df_combined['SALES_Two_Month_ago_Sales_percentage'].round(1)
    ###################################################################################################################################################################################################
    if district =="ALL":
        data=work(df_combined)
        return templates.TemplateResponse("report.html", {"request": request, "data": data, "selected_date": date,"district":district,"data1":data1})
    
    else:
        data={}
        data_imfs=pd.DataFrame()
        data_beer=pd.DataFrame()
        data_sales=pd.DataFrame()        
        data_imfs[["district_name","Shop no","current_month_sales","last_year_sales","current_month_percentage","Last_month_sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","IMFS_Current_Month_Sales","IMFS_Last_Year_Current_Month_Sales","IMFS_Current_Month_Sales_percentage","IMFS_Last_Month_Sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]
        data_beer[["district_name","Shop no","current_month_sales","last_year_sales","current_month_beer_percentage","Last_month_sales","Beer_Last_Year_Last_Month_Sales","beer_last_Month_Sales_percentage","beer_Two_Months_Ago_Sales","beer_Last_Year_Two_Months_Ago_Sales","beer_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","BEER_Current_Month_Sales","BEER_Last_Year_Current_Month_Sales","BEER_Current_Month_Sales_percentage","BEER_Last_Month_Sales","BEER_Last_Year_Last_Month_Sales","BEER_last_Month_Sales_percentage","BEER_Two_Months_Ago_Sales","BEER_Last_Year_Two_Months_Ago_Sales","BEER_Two_Month_ago_Sales_percentage"]]
        data_sales[["district_name","Shop no","current_month_sales","last_year_sales","current_month_sales_percentage","Last_month_sales","sales_Last_Year_Last_Month_Sales","sales_last_Month_Sales_percentage","sales_Two_Months_Ago_Sales","sales_Last_Year_Two_Months_Ago_Sales","sales_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","Sales_Current_Month_Sales","Sales_Last_Year_Current_Month_Sales","SALES_Current_Month_Sales_percentage","Sales_Last_Month_Sales","Sales_Last_Year_Last_Month_Sales","SALES_last_Month_Sales_percentage","Sales_Two_Months_Ago_Sales","Sales_Last_Year_Two_Months_Ago_Sales","SALES_Two_Month_ago_Sales_percentage"]]
        data_imfs["current_month_percentage"] = pd.to_numeric(data_imfs["current_month_percentage"])
        data_beer["current_month_beer_percentage"] = pd.to_numeric(data_beer["current_month_beer_percentage"])
        data_sales["current_month_sales_percentage"] = pd.to_numeric(data_sales["current_month_sales_percentage"])
        district_name=district
        imfs_largest = get_largest_values(data_imfs, district_name, 'current_month_percentage')
        beer_largest = get_largest_values(data_beer, district_name, 'current_month_beer_percentage')
        sales_largest = get_largest_values(data_sales, district_name, 'current_month_sales_percentage')
        imfs_smallest = get_smallest_values(data_imfs, district_name, 'current_month_percentage')
        beer_smallest = get_smallest_values(data_beer, district_name, 'current_month_beer_percentage')
        sales_smalllest = get_smallest_values(data_sales, district_name, 'current_month_sales_percentage')
        data.update({"IMFS_largest":imfs_largest,"IMFS_smallest":imfs_smallest,"BEER_largest":beer_largest,"BEER_smallest":beer_smallest,"SALES_largest":sales_largest,"SALES_smallest":sales_smalllest})
        return templates.TemplateResponse("report.html", {"request": request, "data": data, "selected_date": date,"district":district_name,"data1":data1})
    
@app.post("/report1", response_class=HTMLResponse)
async def generate_report(request: Request, date: str = Form(...),district: str = Form(...)):
    selected_date = d.strptime(date, "%Y-%m-%d").day 
    selected_year = d.strptime(date, "%Y-%m-%d").year
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    month_str = date_obj.strftime("%b")
    year={"year":selected_year,"month":month_str}
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
    
    df["Sales_2023-2022_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2023"], row["SALES_Total_2022"]),axis=1)
    df['Sales_2023-2022_Sales_percentage'] = pd.to_numeric(df['Sales_2023-2022_Sales_percentage'])
    df['Sales_2023-2022_Sales_percentage'] = df['Sales_2023-2022_Sales_percentage'].round(1)

    df["Sales_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2022"], row["SALES_Total_2021"]),axis=1)
    df['Sales_Last_Year_Sales_percentage'] = pd.to_numeric(df['Sales_Last_Year_Sales_percentage'])
    df['Sales_Last_Year_Sales_percentage'] = df['Sales_Last_Year_Sales_percentage'].round(1)
    ##############IMS Percentages
    df["IMFS_Current_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["IMFS_Total_2024"], row["IMFS_Total_2023"]),axis=1)
    df['IMFS_Current_Year_Sales_percentage'] = pd.to_numeric(df['IMFS_Current_Year_Sales_percentage'])
    df['IMFS_Current_Year_Sales_percentage'] = df['IMFS_Current_Year_Sales_percentage'].round(1)
    
    df["IMFS_2023-2022_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["IMFS_Total_2023"], row["IMFS_Total_2022"]),axis=1)
    df['IMFS_2023-2022_Sales_percentage'] = pd.to_numeric(df['IMFS_2023-2022_Sales_percentage'])
    df['IMFS_2023-2022_Sales_percentage'] = df['IMFS_2023-2022_Sales_percentage'].round(1)

    df["IMFS_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["SALES_Total_2022"], row["SALES_Total_2021"]),axis=1)
    df['IMFS_Last_Year_Sales_percentage'] = pd.to_numeric(df['IMFS_Last_Year_Sales_percentage'])
    df['IMFS_Last_Year_Sales_percentage'] = df['IMFS_Last_Year_Sales_percentage'].round(1)
    ################## BEER Percentage
    df["BEER_Current_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["BEER_Total_2024"], row["BEER_Total_2023"]),axis=1)
    df['BEER_Current_Year_Sales_percentage'] = pd.to_numeric(df['BEER_Current_Year_Sales_percentage'])
    df['BEER_Current_Year_Sales_percentage'] = df['BEER_Current_Year_Sales_percentage'].round(1)
    
    df["BEER_2023_2022_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["BEER_Total_2023"], row["BEER_Total_2022"]),axis=1)
    df['BEER_2023_2022_Sales_percentage'] = pd.to_numeric(df['BEER_2023_2022_Sales_percentage'])
    df['BEER_2023_2022_Sales_percentage'] = df['BEER_2023_2022_Sales_percentage'].round(1)   
    
    df["BEER_Last_Year_Sales_percentage"]= df.apply(lambda row: calculate_percentage_change(row["BEER_Total_2022"], row["BEER_Total_2021"]),axis=1)
    df['BEER_Last_Year_Sales_percentage'] = pd.to_numeric(df['BEER_Last_Year_Sales_percentage'])
    df['BEER_Last_Year_Sales_percentage'] = df['BEER_Last_Year_Sales_percentage'].round(1)
    if district=="ALL":
        data=work1(df)        
        return templates.TemplateResponse("report1.html", {"request": request, "data": data, "selected_date": date,"district":district,"year":year})
    else:
        data={}
        district_name=district
        data_imfs=pd.DataFrame()
        data_beer=pd.DataFrame()
        data_sales=pd.DataFrame()
        data_imfs[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_2023-2022_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","IMFS_Total_2024","IMFS_Total_2023","IMFS_Current_Year_Sales_percentage","IMFS_2023-2022_Sales_percentage","IMFS_Total_2022","IMFS_Total_2021","IMFS_Last_Year_Sales_percentage"]]
        data_beer[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_2023_2022_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","BEER_Total_2024","BEER_Total_2023","BEER_Current_Year_Sales_percentage","BEER_2023_2022_Sales_percentage","BEER_Total_2022","BEER_Total_2021","BEER_Last_Year_Sales_percentage"]]
        data_sales[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","SALES_Current_Year_Sales_percentage","Sales_2023-2022_Sales_percentage","SALES_Total_2022","SALES_Total_2021","SALES_Last_Year_Sales_percentage"]]=df[["district_name","shop_no","SALES_Total_2024","SALES_Total_2023","Sales_Current_Year_Sales_percentage","Sales_2023-2022_Sales_percentage","SALES_Total_2022","SALES_Total_2021","Sales_Last_Year_Sales_percentage"]]
        Caputer_objet.Data.update({"imfs":data_imfs,"beer":data_beer,"sales":data_sales})
        imfs_largest = get_largest_values(data_imfs, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_largest = get_largest_values(data_beer, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_largest = get_largest_values(data_sales, district_name, 'SALES_Current_Year_Sales_percentage')
        imfs_smallest = get_smallest_values(data_imfs, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_smallest = get_smallest_values(data_beer, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_smalllest = get_smallest_values(data_sales, district_name, 'SALES_Current_Year_Sales_percentage')
        data.update({"IMFS_largest":imfs_largest,"IMFS_smallest":imfs_smallest,"BEER_largest":beer_largest,"BEER_smallest":beer_smallest,"SALES_largest":sales_largest,"SALES_smallest":sales_smalllest})        
        return templates.TemplateResponse("report1.html", {"request": request, "data": data, "selected_date": date,"district":district_name,"year":year})

@app.post("/report2", response_class=HTMLResponse)
async def generate_report(request: Request, data: str = Form(...)): 
    if data=="ALL":
        datas = Caputer_objet()    
        a = datas.Data["imfs"]
        b = datas.Data["beer"]
        c = datas.Data["sales"]    
        a["combined"] = a['district_name'].astype(str)
        b["combined"] = b['district_name'].astype(str) 
        c["combined"] = c['district_name'].astype(str)   
        a["IMFS_Current_Year_Sales_percentage"] = pd.to_numeric(a["IMFS_Current_Year_Sales_percentage"])
        data_imfs = a.nlargest(5, 'IMFS_Current_Year_Sales_percentage')
        data_imfs1 = a.nsmallest(5, 'IMFS_Current_Year_Sales_percentage')
        data1 = data_imfs['IMFS_Current_Year_Sales_percentage'].tolist() + data_imfs1['IMFS_Current_Year_Sales_percentage'].tolist()
        value1 = data_imfs["combined"].tolist() + data_imfs1["combined"].tolist()   
        b["BEER_Current_Year_Sales_percentage"] = pd.to_numeric(b["BEER_Current_Year_Sales_percentage"])
        data_beer = b.nlargest(5, 'BEER_Current_Year_Sales_percentage')
        data_beer1 = b.nsmallest(5, 'BEER_Current_Year_Sales_percentage')
        data2 = data_beer['BEER_Current_Year_Sales_percentage'].tolist() + data_beer1['BEER_Current_Year_Sales_percentage'].tolist()
        value2 = data_beer["combined"].tolist() + data_beer1["combined"].tolist()    
        c['SALES_Current_Year_Sales_percentage'] = pd.to_numeric(c['SALES_Current_Year_Sales_percentage'])
        data_sales = c.nlargest(5, 'SALES_Current_Year_Sales_percentage')
        data_sales1 = c.nsmallest(5, 'SALES_Current_Year_Sales_percentage')
        data3 = data_sales['SALES_Current_Year_Sales_percentage'].tolist() + data_sales1['SALES_Current_Year_Sales_percentage'].tolist()
        value3 = data_sales["combined"].tolist() + data_sales1["combined"].tolist()
        out = [
            {"v": "IMFS_Current_Year_Sales_percentage", "data": data1, "value": value1},
            {"v": "BEER_Current_Year_Sales_percentage", "data": data2, "value": value2},
            {"v": "SALES_Current_Year_Sales_percentage", "data": data3, "value": value3}
        ]
        folder_path = 'images'
        os.makedirs(folder_path, exist_ok=True)
        for i in out:
            y = i["data"]
            x = i["value"]
            z = i["v"]       
            colors = ['blue'] * 5 + ['red'] * 5        
            plt.figure(figsize=(12, 7))
            bars = plt.bar(range(len(x)), y, color=colors, tick_label=x)
            plt.xlabel('District and Shops')
            plt.ylabel(z)
            plt.title(z)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()        
            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2.0, yval, round(yval, 2), va='bottom', ha='center')
            
            file_path = os.path.join(folder_path, f"{z}.png")
            plt.savefig(file_path)
            plt.clf()

        images = [f"{img}" for img in os.listdir(folder_path) if img.endswith(('png', 'jpg', 'jpeg', 'gif'))]
        return templates.TemplateResponse("report2.html", {"request": request, "images": images})
    else:
        district_name=data
        datas = Caputer_objet()    
        a = datas.Data["imfs"]
        b = datas.Data["beer"]
        c = datas.Data["sales"]    
        a["combined"] = a["shop_no"].astype(str)
        b["combined"] = b["shop_no"].astype(str)
        c["combined"] = c["shop_no"].astype(str)  
        imfs_largest = get_largest_values1(a, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_largest = get_largest_values1(b, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_largest = get_largest_values1(c, district_name, 'SALES_Current_Year_Sales_percentage')
        imfs_smallest = get_smallest_values1(a, district_name, 'IMFS_Current_Year_Sales_percentage')
        beer_smallest = get_smallest_values1(b, district_name, 'BEER_Current_Year_Sales_percentage')
        sales_smalllest = get_smallest_values1(c, district_name, 'SALES_Current_Year_Sales_percentage')      
        data1 = imfs_largest['IMFS_Current_Year_Sales_percentage'].tolist() + imfs_smallest['IMFS_Current_Year_Sales_percentage'].tolist()
        value1 = imfs_largest["combined"].tolist() + imfs_smallest["combined"].tolist()
        data2 = beer_largest['BEER_Current_Year_Sales_percentage'].tolist() + beer_smallest['BEER_Current_Year_Sales_percentage'].tolist()
        value2 = beer_largest["combined"].tolist() + beer_smallest["combined"].tolist()
        data3 = sales_largest['SALES_Current_Year_Sales_percentage'].tolist() + sales_smalllest['SALES_Current_Year_Sales_percentage'].tolist()
        value3 = sales_largest["combined"].tolist() + sales_smalllest["combined"].tolist()
        out = [
            {"v": "IMFS_Current_Year_Sales_percentage", "data": data1, "value": value1},
            {"v": "BEER_Current_Year_Sales_percentage", "data": data2, "value": value2},
            {"v": "SALES_Current_Year_Sales_percentage", "data": data3, "value": value3}
        ]
        folder_path = 'images'
        os.makedirs(folder_path, exist_ok=True)
        for i in out:
            y = i["data"]
            x = i["value"]
            z = i["v"]       
            colors = ['blue'] * 5 + ['red'] * 5        
            plt.figure(figsize=(12, 7))
            bars = plt.bar(range(len(x)), y, color=colors, tick_label=x)
            plt.xlabel('District and Shops')
            plt.ylabel(z)
            plt.title(z)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()        
            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2.0, yval, round(yval, 2), va='bottom', ha='center')
            
            file_path = os.path.join(folder_path, f"{z}.png")
            plt.savefig(file_path)
            plt.clf()

        images = [f"{img}" for img in os.listdir(folder_path) if img.endswith(('png', 'jpg', 'jpeg', 'gif'))]
        return templates.TemplateResponse("report2.html", {"request": request, "images": images})


