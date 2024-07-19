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
def calculate_percentage_change(current, previous):
    if previous == 0:
        return 0 
    return ((current - previous) / previous) * 100

mycursor = mydb.cursor()

# Execute the SQL query
mycursor.execute("""WITH SalesTotals AS (
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
        AND DAY(date) BETWEEN 1 AND 8
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

# Fetch the result
myresult = mycursor.fetchall()

# Get column names from the cursor
column_names = [desc[0] for desc in mycursor.description]

# Create a DataFrame
df_combined = pd.DataFrame(myresult, columns=column_names)
############################### IMFS ########################################################################
###current Month IMFS
df_combined["IMFS_Current_Month_Sales_percentage"]= df_combined.apply(lambda row: calculate_percentage_change(row["IMFS_Current_Month_Sales"], row["IMFS_Last_Year_Current_Month_Sales"]),axis=1)
df_combined['IMFS_Current_Month_Sales_percentage'] = df_combined['IMFS_Current_Month_Sales_percentage'].round(1)
###### Last Month IMFS
df_combined["IMFS_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['IMFS_Last_Month_Sales'] , row['IMFS_Last_Year_Last_Month_Sales'] ),axis=1)
df_combined['IMFS_last_Month_Sales_percentage'] = df_combined['IMFS_last_Month_Sales_percentage'].round(1)
####### Two Month  AGO IMFS
df_combined["IMFS_Two_Month_ago_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change(row['IMFS_Two_Months_Ago_Sales'] , row['IMFS_Last_Year_Two_Months_Ago_Sales']),axis=1) 
df_combined['IMFS_Two_Month_ago_Sales_percentage'] = df_combined['IMFS_Two_Month_ago_Sales_percentage'].round(1)
#################################################################################################################################################################################
#################################### BEER #########################################################################################################################################
###current Month IMFS
df_combined["BEER_Current_Month_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change (row['BEER_Current_Month_Sales'], row['BEER_Last_Year_Current_Month_Sales'] ),axis=1) 
df_combined['BEER_Current_Month_Sales_percentage'] = df_combined['BEER_Current_Month_Sales_percentage'].round(1)
###### Last Month IMFS
df_combined["BEER_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['BEER_Last_Month_Sales'] , row['BEER_Last_Year_Last_Month_Sales']),axis=1)     
df_combined['BEER_last_Month_Sales_percentage'] = df_combined['BEER_last_Month_Sales_percentage'].round(1)
####### Two Month  AGO IMFS
df_combined["BEER_Two_Month_ago_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['BEER_Two_Months_Ago_Sales'],row['BEER_Last_Year_Two_Months_Ago_Sales']),axis=1)
df_combined['BEER_Two_Month_ago_Sales_percentage'] = df_combined['BEER_Two_Month_ago_Sales_percentage'].round(1)
#########################################################################################################################################################################################
################################################################# SALES REPORT ####################################################################################################################
df_combined["SALES_Current_Month_Sales_percentage"]=df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Current_Month_Sales'] , row['Sales_Last_Year_Current_Month_Sales'] ),axis=1)
df_combined['SALES_Current_Month_Sales_percentage'] = df_combined['SALES_Current_Month_Sales_percentage'].round(1)
###### Last Month IMFS
df_combined["SALES_last_Month_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Last_Month_Sales'],row['Sales_Last_Year_Last_Month_Sales']),axis=1 ) 
df_combined['SALES_last_Month_Sales_percentage'] = df_combined['SALES_last_Month_Sales_percentage'].round(1)
####### Two Month  AGO IMFS
df_combined["SALES_Two_Month_ago_Sales_percentage"]= df_combined.apply(lambda row:calculate_percentage_change(row['Sales_Two_Months_Ago_Sales'], row['Sales_Last_Year_Two_Months_Ago_Sales']),axis=1)
df_combined['SALES_Two_Month_ago_Sales_percentage'] = df_combined['SALES_Two_Month_ago_Sales_percentage'].round(1)
###################################################################################################################################################################################################
data_imfs=pd.DataFrame()
data_beer=pd.DataFrame()
data_sales=pd.DataFrame()
data_imfs[["district_name","Shop no","current_month_sales","last_year_sales","current_month_percentage","Last_month_sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","IMFS_Current_Month_Sales","IMFS_Last_Year_Current_Month_Sales","IMFS_Current_Month_Sales_percentage","IMFS_Last_Month_Sales","IMFS_Last_Year_Last_Month_Sales","IMFS_last_Month_Sales_percentage","IMFS_Two_Months_Ago_Sales","IMFS_Last_Year_Two_Months_Ago_Sales","IMFS_Two_Month_ago_Sales_percentage"]]
data_beer[["district_name","Shop no","current_month_sales","last_year_sales","current_month_beer_percentage","Last_month_sales","Beer_Last_Year_Last_Month_Sales","beer_last_Month_Sales_percentage","beer_Two_Months_Ago_Sales","beer_Last_Year_Two_Months_Ago_Sales","beer_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","BEER_Current_Month_Sales","BEER_Last_Year_Current_Month_Sales","BEER_Current_Month_Sales_percentage","BEER_Last_Month_Sales","BEER_Last_Year_Last_Month_Sales","BEER_last_Month_Sales_percentage","BEER_Two_Months_Ago_Sales","BEER_Last_Year_Two_Months_Ago_Sales","BEER_Two_Month_ago_Sales_percentage"]]
data_sales[["district_name","Shop no","current_month_sales","last_year_sales","current_month_percentage","Last_month_sales","sales_Last_Year_Last_Month_Sales","sales_last_Month_Sales_percentage","sales_Two_Months_Ago_Sales","sales_Last_Year_Two_Months_Ago_Sales","sales_Two_Month_ago_Sales_percentage"]]=df_combined[["district_name","shop_no","Sales_Current_Month_Sales","Sales_Last_Year_Current_Month_Sales","SALES_Current_Month_Sales_percentage","Sales_Last_Month_Sales","Sales_Last_Year_Last_Month_Sales","SALES_last_Month_Sales_percentage","Sales_Two_Months_Ago_Sales","Sales_Last_Year_Two_Months_Ago_Sales","SALES_Two_Month_ago_Sales_percentage"]]
# a=data_imfs.nlargest(10, 'current_month_percentage')
data_imfs["current_month_percentage"]=pd.to_numeric(df_combined["IMFS_Current_Month_Sales_percentage"])
top_10_imfs = data_imfs.nlargest(10, 'current_month_percentage')
data_beer["current_month_beer_percentage"]=pd.to_numeric(df_combined["BEER_Current_Month_Sales_percentage"])
b=data_beer.nsmallest(10, 'current_month_beer_percentage')
out={}
def get_largest_values(data, district, column):
    filtered_data = data[data["district_name"] == district]
    largest = filtered_data.nlargest(10, column)
    return largest.to_dict(orient='records')

# Update percentage columns to numeric
data_imfs["current_month_percentage"] = pd.to_numeric(data_imfs["current_month_percentage"])
data_beer["current_month_beer_percentage"] = pd.to_numeric(data_beer["current_month_beer_percentage"])
data_sales["current_month_sales_percentage"] = pd.to_numeric(data_sales["current_month_sales_percentage"])

# Replace 'District_Name' with the actual district name you want to filter by
district_name = 'Chennai Central'

# Get largest values for the specific district
imfs_largest = get_largest_values(data_imfs, district_name, 'current_month_percentage')
beer_largest = get_largest_values(data_beer, district_name, 'current_month_beer_percentage')
sales_largest = get_largest_values(data_sales, district_name, 'current_month_sales_percentage')

out.update({"IMFS_largest": imfs_largest})
out.update({"BEER_largest": beer_largest})
# out.update({"SALES_largest": sales_largest})

print(out)
