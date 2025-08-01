import sqlite3
import logging
import pandas as pd
from data_ingestion import ingest_db
logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)
def create_vendor_summary(conn):
    """this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data"""
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary as (
    select VendorNumber,
    sum(Freight) as FreightCost
    from vendor_invoice
    Group by VendorNumber
),

PurchaseSummary as (
    select p.VendorNumber,
    p.VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Price as ActualPrice,
    pp.Volume,
    sum(p.Quantity) as TotalPurchaseQuantity,
    sum(p.Dollars) as TotalPurchaseDollars
    from purchases p
    join purchase_prices pp
    on p.Brand  = pp.Brand
    where p.PurchasePrice > 0
    Group by p.VendorNumber , p.VendorName , p.Brand , p.Description , p.PurchasePrice , ActualPrice , pp.Volume
    
),

SalesSummary as (
    select VendorNo,
    Brand,
    sum(SalesQuantity) as TotalSalesQuantity,
    sum(SalesDollars) as TotalSalesDollars,
    sum(SalesPrice) as totalSalesPrice,
    sum(ExciseTax) as TotalExciseTax
    from sales
    Group by  VendorNo , Brand
)

select ps.VendorNumber,
ps.VendorName,
ps.Brand,
ps.Description,
ps.PurchasePrice,
ps.ActualPrice,
ps.Volume,
ps.TotalPurchaseQuantity,
ps.TotalPurchaseDollars,
ss.TotalSalesQuantity,
ss.TotalSalesDollars,
ss.TotalSalesPrice,
ss.TotalExciseTax,
fs.FreightCost
from PurchaseSummary ps
left join SalesSummary ss
on ps.VendorNumber = ss.VendorNo
and ps.Brand = ss.Brand
left join FreightSummary fs
on ps.VendorNumber  = fs.VendorNumber
order by ps.TotalPurchaseDollars DESC
""" , conn)
    return vendor_sales_summary


def clean_data(vendor_sales_summary)    :
    """this function will clean the data"""
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float64')
    vendor_sales_summary.fillna(0 , inplace  = True)
    vendor_sales_summary['VendorName']  = vendor_sales_summary['VendorName'].str.strip()
    vendor_sales_summary['Description']  = vendor_sales_summary['Description'].str.strip()
    
    vendor_sales_summary['GrossProfit']  = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnOver']  = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    return vendor_sales_summary



if __name__=='__main__':
    # creating database connection
    conn = sqlite3.connect('inventory.db')
    logging.info('Creating Vendor Summary Table....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())
    
    
    logging.info('Cleaning Data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    
    
    logging.info('Ingesting data...')
    ingest_db(clean_df , 'vendor_sales_summary' , conn)
    logging.info('Completed')