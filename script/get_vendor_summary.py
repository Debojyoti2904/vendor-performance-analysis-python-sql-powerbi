import sqlite3
import pandas as pd
import logging

logging.basicConfig(
    filename="logs/ingestion.db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s"
    filemode = "a"
)

def create_vendor_summary(conn):
    vendor_sales_summary = pd.read_sql_query("""WITH Freight_Summary AS (
    SELECT 
        VendorNumber,
        SUM(Freight) AS Freight_Cost
    FROM vendor_invoice
    GROUP BY VendorNumber
),

PurchaseSummary AS (
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Volume,
        pp.Price as Actual_Price,
        SUM(p.Quantity) as Total_Purchased_Quantity,
        SUM(p.Dollars) as Total_Purchased_Dollars
    FROM purchases p
    JOIN purchase_prices pp ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.PurchasePrice, pp.Price, pp.Volume
),

SalesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesDollars) as Total_SalesDollars,
        SUM(SalesPrice) as Total_SalesPrice,
        SUM(SalesQuantity) as Total_SalesQuantity,
        SUM(ExciseTax) as Total_ExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
)

SELECT 
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.Actual_Price,
    ps.Volume,
    ps.Total_Purchased_Quantity,
    ps.Total_Purchased_Dollars,
    ss.Total_SalesDollars,
    ss.Total_SalesQuantity,
    ss.Total_SalesPrice,
    ss.Total_ExciseTax,
    fs.Freight_Cost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN Freight_Summary fs
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.Total_Purchased_Dollars DESC""", conn)
    
    return vendor_sales_summary

def clean_data(df):
    df["Volume"] = df["Volume"].astype('float64')
    
    #filling missing value with 0
    df.fillna(0, inplace = True)
    
    #removing spaces from categorical columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()
    
    
    