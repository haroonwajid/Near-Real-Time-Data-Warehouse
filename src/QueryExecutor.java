import java.sql.*;

public class QueryExecutor {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/MetroDW";
    private static final String USER = "root"; // Replace with your MySQL username
    private static final String PASS = "password"; // Replace with your MySQL password

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {
            System.out.println("Connected to the database.");

            // Execute each query
            executeQuery1(connection);
            executeQuery2(connection);
            executeQuery3(connection);
            executeQuery4(connection);
            executeQuery5(connection);
            executeQuery6(connection);
            executeQuery7(connection);
            executeQuery8(connection);
            executeQuery9(connection);
            executeQuery10(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeQuery1(Connection connection) throws SQLException {
        String query = "SELECT MONTH(Order_Date) AS Month, " +
                       "CASE WHEN DAYOFWEEK(Order_Date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS Day_Type, " +
                       "P.Product_Name, SUM(SF.Total_Sale) AS Total_Revenue " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "WHERE YEAR(Order_Date) = 2019 " +
                       "GROUP BY Month, Day_Type, P.Product_Name " +
                       "ORDER BY Month, Day_Type, Total_Revenue DESC " +
                       "LIMIT 5;";
        executeAndPrintResults(connection, query, "Query 1");
    }

    private static void executeQuery2(Connection connection) throws SQLException {
        String query = "SELECT QUARTER(SF.Order_Date) AS Quarter, P.Product_Name, " +
                       "SUM(SF.Total_Sale) AS Total_Revenue, " +
                       "(SUM(SF.Total_Sale) - LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Product_ID ORDER BY QUARTER(SF.Order_Date))) / " +
                       "LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Product_ID ORDER BY QUARTER(SF.Order_Date)) * 100 AS Growth_Rate " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "WHERE YEAR(SF.Order_Date) = 2019 " +
                       "GROUP BY P.Product_ID, Quarter " +
                       "ORDER BY P.Product_ID, Quarter;";
        executeAndPrintResults(connection, query, "Query 2");
    }

    private static void executeQuery3(Connection connection) throws SQLException {
        String query = "SELECT P.Supplier_Name, P.Product_Name, SUM(SF.Total_Sale) AS Total_Sales " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Supplier_Name, P.Product_Name " +
                       "ORDER BY P.Supplier_Name, Total_Sales DESC;";
        executeAndPrintResults(connection, query, "Query 3");
    }

    private static void executeQuery4(Connection connection) throws SQLException {
        String query = "SELECT P.Product_Name, " +
                       "CASE WHEN MONTH(SF.Order_Date) IN (3, 4, 5) THEN 'Spring' " +
                       "WHEN MONTH(SF.Order_Date) IN (6, 7, 8) THEN 'Summer' " +
                       "WHEN MONTH(SF.Order_Date) IN (9, 10, 11) THEN 'Fall' " +
                       "ELSE 'Winter' END AS Season, " +
                       "SUM(SF.Total_Sale) AS Total_Sales " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Product_Name, Season " +
                       "ORDER BY P.Product_Name, Season;";
        executeAndPrintResults(connection, query, "Query 4");
    }

    private static void executeQuery5(Connection connection) throws SQLException {
        String query = "SELECT P.Supplier_Name, P.Product_Name, MONTH(SF.Order_Date) AS Month, " +
                       "SUM(SF.Total_Sale) AS Total_Revenue, " +
                       "(SUM(SF.Total_Sale) - LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Supplier_Name, P.Product_ID ORDER BY MONTH(SF.Order_Date))) / " +
                       "LAG(SUM(SF.Total_Sale)) OVER (PARTITION BY P.Supplier_Name, P.Product_ID ORDER BY MONTH(SF.Order_Date)) * 100 AS Volatility " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Supplier_Name, P.Product_ID, Month " +
                       "ORDER BY P.Supplier_Name, P.Product_ID, Month;";
        executeAndPrintResults(connection, query, "Query 5");
    }

    private static void executeQuery6(Connection connection) throws SQLException {
        String query = "SELECT SF1.Product_ID AS Product1_ID, SF2.Product_ID AS Product2_ID, COUNT(*) AS Frequency " +
                       "FROM Sales_Fact SF1 " +
                       "JOIN Sales_Fact SF2 ON SF1.Order_ID = SF2.Order_ID AND SF1.Product_ID < SF2.Product_ID " +
                       "GROUP BY SF1.Product_ID, SF2.Product_ID " +
                       "ORDER BY Frequency DESC " +
                       "LIMIT 5;";
        executeAndPrintResults(connection, query, "Query 6");
    }

    private static void executeQuery7(Connection connection) throws SQLException {
        String query = "SELECT P.Supplier_Name, P.Product_Name, YEAR(SF.Order_Date) AS Year, " +
                       "SUM(SF.Total_Sale) AS Total_Revenue " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Supplier_Name, P.Product_Name, Year WITH ROLLUP " +
                       "ORDER BY P.Supplier_Name, P.Product_Name, Year;";
        executeAndPrintResults(connection, query, "Query 7");
    }

    private static void executeQuery8(Connection connection) throws SQLException {
        String query = "SELECT P.Product_Name, " +
                       "CASE WHEN MONTH(SF.Order_Date) BETWEEN 1 AND 6 THEN 'H1' ELSE 'H2' END AS Half_Year, " +
                       "SUM(SF.Total_Sale) AS Total_Revenue, SUM(SF.Quantity) AS Total_Quantity " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Product_Name, Half_Year " +
                       "ORDER BY P.Product_Name, Half_Year;";
        executeAndPrintResults(connection, query, "Query 8");
    }

    private static void executeQuery9(Connection connection) throws SQLException {
        String query = "WITH Daily_Averages AS ( " +
                       "SELECT P.Product_ID, P.Product_Name, AVG(SF.Total_Sale) AS Daily_Avg_Sale " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "GROUP BY P.Product_ID, P.Product_Name " +
                       ") " +
                       "SELECT P.Product_Name, SF.Order_Date, SUM(SF.Total_Sale) AS Daily_Sale, DA.Daily_Avg_Sale, " +
                       "CASE WHEN SUM(SF.Total_Sale) > 2 * DA.Daily_Avg_Sale THEN 'Outlier' ELSE 'Normal' END AS Status " +
                       "FROM Sales_Fact SF " +
                       "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                       "JOIN Daily_Averages DA ON SF.Product_ID = DA.Product_ID " +
                       "GROUP BY P.Product_Name, SF.Order_Date, DA.Daily_Avg_Sale " +
                       "ORDER BY P.Product_Name, SF.Order_Date;";
        executeAndPrintResults(connection, query, "Query 9");
    }

    private static void executeQuery10(Connection connection) throws SQLException {
        String createViewQuery = "CREATE OR REPLACE VIEW PRODUCT_QUARTERLY_SALES AS " +
                                 "SELECT P.Product_Name, QUARTER(SF.Order_Date) AS Quarter, " +
                                 "SUM(SF.Total_Sale) AS Total_Quarterly_Sales " +
                                 "FROM Sales_Fact SF " +
                                 "JOIN Products_Dim P ON SF.Product_ID = P.Product_ID " +
                                 "GROUP BY P.Product_Name, Quarter " +
                                 "ORDER BY P.Product_Name, Quarter;";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createViewQuery);
            System.out.println("View PRODUCT_QUARTERLY_SALES created successfully.");
        }

        String selectViewQuery = "SELECT * FROM PRODUCT_QUARTERLY_SALES;";
        executeAndPrintResults(connection, selectViewQuery, "Query 10");
    }

    private static void executeAndPrintResults(Connection connection, String query, String queryName) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            System.out.println("Results for " + queryName + ":");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
                }
                System.out.println();
            }
            System.out.println();
        }
    }
} 