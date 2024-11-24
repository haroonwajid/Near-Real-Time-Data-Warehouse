use MetroDW;
-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT 
    MONTH(Order_Date) AS Month,
    CASE 
        WHEN DAYOFWEEK(Order_Date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS Day_Type,
    P.Product_Name,
    SUM(SF.Total_Sale) AS Total_Revenue
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
WHERE 
    YEAR(Order_Date) = 2019 
GROUP BY 
    Month, Day_Type, P.Product_Name
ORDER BY 
    Month, Day_Type, Total_Revenue DESC
LIMIT 5;

-- Q2. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
SELECT 
    QUARTER(SF.Order_Date) AS Quarter,
    P.Product_Name,
    SUM(SF.Total_Sale) AS Total_Revenue,
    (SUM(SF.Total_Sale) - LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Product_ID ORDER BY QUARTER(SF.Order_Date))) / LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Product_ID ORDER BY QUARTER(SF.Order_Date)) * 100 AS Growth_Rate
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
WHERE 
    YEAR(SF.Order_Date) = 2019
GROUP BY 
    P.Product_ID, Quarter
ORDER BY 
    P.Product_ID, Quarter;

-- Q3. Detailed Supplier Sales Contribution by Store and Product Name
SELECT 
    P.Supplier_Name,
    P.Product_Name,
    SUM(SF.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Supplier_Name, P.Product_Name
ORDER BY 
    P.Supplier_Name, Total_Sales DESC;

-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT 
    P.Product_Name,
    CASE 
        WHEN MONTH(SF.Order_Date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(SF.Order_Date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(SF.Order_Date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS Season,
    SUM(SF.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Product_Name, Season
ORDER BY 
    P.Product_Name, Season;

-- Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
SELECT 
    P.Supplier_Name,
    P.Product_Name,
    MONTH(SF.Order_Date) AS Month,
    SUM(SF.Total_Sale) AS Total_Revenue,
    (SUM(SF.Total_Sale) - LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Supplier_Name, P.Product_ID ORDER BY MONTH(SF.Order_Date))) / LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Supplier_Name, P.Product_ID ORDER BY MONTH(SF.Order_Date)) * 100 AS Volatility
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Supplier_Name, P.Product_ID, Month
ORDER BY 
    P.Supplier_Name, P.Product_ID, Month;

-- Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
SELECT 
    SF1.Product_ID AS Product1_ID,
    SF2.Product_ID AS Product2_ID,
    COUNT(*) AS Frequency
FROM 
    Sales_Fact SF1
JOIN 
    Sales_Fact SF2 ON SF1.Order_ID = SF2.Order_ID AND SF1.Product_ID < SF2.Product_ID
GROUP BY 
    SF1.Product_ID, SF2.Product_ID
ORDER BY 
    Frequency DESC
LIMIT 5;

-- Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT 
    P.Supplier_Name,
    P.Product_Name,
    YEAR(SF.Order_Date) AS Year,
    SUM(SF.Total_Sale) AS Total_Revenue
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Supplier_Name, P.Product_Name, Year WITH ROLLUP
ORDER BY 
    P.Supplier_Name, P.Product_Name, Year;

-- Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
SELECT 
    P.Product_Name,
    CASE 
        WHEN MONTH(SF.Order_Date) BETWEEN 1 AND 6 THEN 'H1'
        ELSE 'H2'
    END AS Half_Year,
    SUM(SF.Total_Sale) AS Total_Revenue,
    SUM(SF.Quantity) AS Total_Quantity
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Product_Name, Half_Year
ORDER BY 
    P.Product_Name, Half_Year;

-- Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers
WITH Daily_Averages AS (
    SELECT 
        P.Product_ID,
        P.Product_Name,
        AVG(SF.Total_Sale) AS Daily_Avg_Sale
    FROM 
        Sales_Fact SF
    JOIN 
        Products_Dim P ON SF.Product_ID = P.Product_ID
    GROUP BY 
        P.Product_ID, P.Product_Name
)
SELECT 
    P.Product_Name,
    SF.Order_Date,
    SUM(SF.Total_Sale) AS Daily_Sale,
    DA.Daily_Avg_Sale,
    CASE 
        WHEN SUM(SF.Total_Sale) > 2 * DA.Daily_Avg_Sale THEN 'Outlier'
        ELSE 'Normal'
    END AS Status
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
JOIN 
    Daily_Averages DA ON SF.Product_ID = DA.Product_ID
GROUP BY 
    P.Product_Name, SF.Order_Date, DA.Daily_Avg_Sale
ORDER BY 
    P.Product_Name, SF.Order_Date;

-- Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
CREATE OR REPLACE VIEW PRODUCT_QUARTERLY_SALES AS
SELECT 
    P.Product_Name,
    QUARTER(SF.Order_Date) AS Quarter,
    SUM(SF.Total_Sale) AS Total_Quarterly_Sales
FROM 
    Sales_Fact SF
JOIN 
    Products_Dim P ON SF.Product_ID = P.Product_ID
GROUP BY 
    P.Product_Name, Quarter
ORDER BY 
    P.Product_Name, Quarter;
    Select * from PRODUCT_QUARTERLY_SALES;