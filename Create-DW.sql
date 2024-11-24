DROP DATABASE IF EXISTS MetroDW;
CREATE DATABASE MetroDW;
USE MetroDW;

-- Drop tables if they already exist
DROP TABLE IF EXISTS Sales_Fact;
DROP TABLE IF EXISTS Products_Dim;
DROP TABLE IF EXISTS Customers_Dim;

-- Create Dimension Tables
CREATE TABLE Products_Dim (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(255),
    Supplier_ID INT,
    Supplier_Name VARCHAR(255),
    Product_Price DECIMAL(10, 2)
);

CREATE TABLE Customers_Dim (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Gender VARCHAR(10)
);

-- Create Fact Table
CREATE TABLE Sales_Fact (
    Order_ID BIGINT PRIMARY KEY,
    Order_Date DATE,
    Order_Time TIME,
    Product_ID INT,
    Customer_ID INT,
    Quantity INT,
    Product_Price DECIMAL(10, 2),
    Total_Sale DECIMAL(10, 2),
    FOREIGN KEY (Product_ID) REFERENCES Products_Dim(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customers_Dim(Customer_ID)
);
