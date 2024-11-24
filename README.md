# Near Real-Time Data Warehouse Project

## Description

This project implements a near real-time data warehouse solution designed to efficiently process and store streaming data. It uses the `MeshJoin` algorithm to join streaming data with static reference data, ensuring minimal latency in data processing. The project includes SQL scripts for creating and querying the data warehouse, as well as Java programs for data ingestion and query execution.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Schema](#schema)
- [Installation](#installation)
- [Usage](#usage)
- [Connecting with SQL Workbench](#connecting-with-sql-workbench)
- [Data Files](#data-files)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Features

- **Near Real-Time Processing**: Efficiently handles high-velocity data streams.
- **Scalable Architecture**: Designed to scale horizontally for growing data volumes.
- **Efficient Data Joining**: Utilizes the `MeshJoin` algorithm for joining streaming and static data.
- **Comprehensive OLAP Queries**: Includes a set of OLAP queries for detailed data analysis.
- **Extensible Framework**: Easily extendable to integrate with various data sources and sinks.

## Architecture

The architecture of this project is designed to support near real-time data processing. Key components include:

- **Data Ingestion**: Uses Java programs to load data from CSV files into the data warehouse.
- **Data Processing**: Processes data using the `MeshJoin` algorithm.
- **Data Storage**: Stores processed data in a MySQL data warehouse.
- **Data Access**: Provides SQL scripts for querying and analyzing data.

## Schema

The data warehouse schema is defined in the `Create-DW.sql` file and includes the following tables:

### Dimension Tables

- **Products_Dim**: Stores product details.
  - `Product_ID`: Primary key, integer.
  - `Product_Name`: Name of the product, varchar.
  - `Supplier_ID`: ID of the supplier, integer.
  - `Supplier_Name`: Name of the supplier, varchar.
  - `Product_Price`: Price of the product, decimal.

- **Customers_Dim**: Stores customer information.
  - `Customer_ID`: Primary key, integer.
  - `Customer_Name`: Name of the customer, varchar.
  - `Gender`: Gender of the customer, varchar.

### Fact Table

- **Sales_Fact**: Stores sales transactions.
  - `Order_ID`: Primary key, bigint.
  - `Order_Date`: Date of the order, date.
  - `Order_Time`: Time of the order, time.
  - `Product_ID`: Foreign key referencing `Products_Dim`.
  - `Customer_ID`: Foreign key referencing `Customers_Dim`.
  - `Quantity`: Quantity of products sold, integer.
  - `Product_Price`: Price of the product at the time of sale, decimal.
  - `Total_Sale`: Total sale amount, decimal.

## Installation

To set up the project, follow these steps:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/haroonwajid/near-real-time-data-warehouse.git
   cd near-real-time-data-warehouse
   ```

2. **Install Dependencies**:
   Ensure you have Java and MySQL installed. Then, build the project using Maven:
   ```bash
   mvn clean install
   ```

3. **Set Up the Database**:
   - Run the `Create-DW.sql` script to create the database and tables:
     ```bash
     mysql -u yourusername -p < Create-DW.sql
     ```

4. **Configure Environment**:
   Set up your environment variables and configuration files as needed for your data sources and sinks.

## Usage

### Running the MeshJoin Program

To run the `MeshJoin` program, which loads data into the data warehouse, execute the following command:

### Executing OLAP Queries

To execute the OLAP queries, run the `QueryExecutor` program:

## Connecting with SQL Workbench

To connect to your MySQL database using SQL Workbench, follow these steps:

1. **Open SQL Workbench**: Launch the SQL Workbench application.

2. **Create a New Connection Profile**:
   - Click on the "File" menu and select "New Connection Profile".
   - Enter a name for your connection profile (e.g., "MetroDW").

3. **Configure Connection Settings**:
   - **Driver**: Select "MySQL" from the dropdown list.
   - **URL**: Enter the connection URL in the format:
     ```
     jdbc:mysql://localhost:3306/MetroDW
     ```
   - **Username**: Enter your MySQL username (e.g., `root`).
   - **Password**: Enter your MySQL password.

4. **Test the Connection**:
   - Click on the "Test" button to verify that the connection settings are correct. If successful, you will see a confirmation message.

5. **Connect to the Database**:
   - Click "OK" to save the connection profile.
   - Select your newly created connection profile from the list and click "Connect".

6. **Run Queries**: You can now run SQL queries against your `MetroDW` database.

## Data Files

The project uses the following data files located in the `data` directory:

- `customers_data.csv`: Contains customer information.
- `products_data.csv`: Contains product details, including prices and supplier information.
- `transactions.csv`: Contains transaction data to be processed and loaded into the data warehouse.

## Contributing

We welcome contributions to this project. To contribute:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Commit your changes and push to your branch.
4. Submit a pull request with a detailed description of your changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
