import java.io.*;
import java.sql.*;
import java.util.*;
import java.math.BigDecimal;

public class MeshJoin {
    private static final int BUFFER_SIZE = 10; // Example buffer size
    private static Connection connection;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Prompt user for database connection details
        String dbURL = "jdbc:mysql://localhost:3306/MetroDW";

        System.out.print("Enter database username: ");
        String username = scanner.nextLine();

        System.out.print("Enter database password: ");
        String password = scanner.nextLine();

        try {
            // Register the JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(dbURL, username, password);
            System.out.println("Database connected successfully.");

            // Execute QueryExecutor
            QueryExecutor.main(args); // Call to execute QueryExecutor

            runMeshJoin();
        } catch (SQLException | IOException | ClassNotFoundException e) {
            System.err.println("Failed to connect to the database or run the mesh join.");
            e.printStackTrace();
            return; // Exit if connection fails
        } finally {
            scanner.close();
        }
    }

    private static void runMeshJoin() throws SQLException, IOException {
        if (connection == null) {
            throw new SQLException("Database connection is not established.");
        }

        // Load data from CSV files
        List<Map<String, Object>> masterDataCustomers = loadCSVData("../data/customers_data.csv");
        List<Map<String, Object>> masterDataProducts = processProductData("../data/products_data.csv");

        // Insert data into dimension tables
        insertCustomers(masterDataCustomers);
        insertProducts(masterDataProducts);

        // Read transactions and populate transactionQueue
        Queue<Map<String, Object>> transactionQueue = new LinkedList<>();
        BufferedReader transactionReader = new BufferedReader(new FileReader("../data/transactions.csv")); // Adjust path as necessary
        String line;
        transactionReader.readLine(); // Skip header row

        while ((line = transactionReader.readLine()) != null) {
            String[] fields = line.split(",");
            Map<String, Object> transaction = new HashMap<>();
            transaction.put("ORDER_ID", fields[0]);
            transaction.put("ORDER_DATE", fields[1]);
            transaction.put("PRODUCT_ID", fields[2]);
            transaction.put("CUSTOMER_ID", fields[3]);
            transaction.put("QUANTITY", Integer.parseInt(fields[4]));

            transactionQueue.add(transaction);

            // Process the buffer when it reaches the defined size
            if (transactionQueue.size() >= BUFFER_SIZE) {
                processBuffer(transactionQueue, masterDataCustomers, masterDataProducts);
            }
        }

        // Process any remaining transactions in the queue
        if (!transactionQueue.isEmpty()) {
            processBuffer(transactionQueue, masterDataCustomers, masterDataProducts);
        }

        transactionReader.close();
        System.out.println("Data loaded into the data warehouse successfully.");
    }

    private static List<Map<String, Object>> loadCSVData(String fileName) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        String[] headers = reader.readLine().split(","); // Read header row

        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            Map<String, Object> record = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                record.put(headers[i], fields[i]);
            }
            data.add(record);
        }

        reader.close();
        return data;
    }

    private static List<Map<String, Object>> processProductData(String fileName) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        String[] headers = reader.readLine().split(","); // Read header row

        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            Map<String, Object> record = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equalsIgnoreCase("productPrice")) {
                    try {
                        // Remove the dollar sign and parse as double
                        String price = fields[i].replace("$", "").trim();
                        record.put(headers[i], Double.parseDouble(price));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid price for product ID: " + fields[0] + " in line: " + line);
                        record.put(headers[i], null); // Set to null if parsing fails
                    }
                } else {
                    record.put(headers[i], fields[i].trim());
                }
            }
            data.add(record);
        }

        reader.close();
        return data;
    }

    private static void processBuffer(Queue<Map<String, Object>> transactionQueue,
                                      List<Map<String, Object>> customers,
                                      List<Map<String, Object>> products) throws SQLException {
        String sqlInsert = "INSERT INTO Sales_Fact (Order_ID, Order_Date, Order_Time, Product_ID, Customer_ID, Quantity, Product_Price, Total_Sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement insertStatement = connection.prepareStatement(sqlInsert)) {
            while (!transactionQueue.isEmpty()) {
                Map<String, Object> transaction = transactionQueue.poll();

                String orderIdStr = (String) transaction.get("ORDER_ID");
                String orderDateTime = (String) transaction.get("ORDER_DATE");
                String productIdStr = (String) transaction.get("PRODUCT_ID");
                String customerIdStr = (String) transaction.get("CUSTOMER_ID");
                int quantity = (int) transaction.get("QUANTITY");

                // Check if the customer and product exist
                boolean customerExists = customers.stream()
                        .anyMatch(customer -> customerIdStr.equals(customer.get("customer_id").toString()));
                Optional<Map<String, Object>> productOpt = products.stream()
                        .filter(product -> productIdStr.equals(product.get("productID").toString()))
                        .findFirst();

                if (customerExists && productOpt.isPresent()) {
                    Map<String, Object> product = productOpt.get();
                    Double productPrice = (Double) product.get("productPrice");

                    if (productPrice == null) {
                        System.err.println("Skipping transaction due to null product price for Product_ID: " + productIdStr);
                        continue;
                    }

                    double totalSale = productPrice * quantity;

                    try {
                        String[] dateTimeParts = orderDateTime.split(" ");
                        String orderDate = dateTimeParts[0];
                        String orderTime = dateTimeParts[1];

                        insertStatement.setInt(1, Integer.parseInt(orderIdStr));
                        insertStatement.setDate(2, java.sql.Date.valueOf(orderDate));
                        insertStatement.setTime(3, java.sql.Time.valueOf(orderTime));
                        insertStatement.setInt(4, Integer.parseInt(productIdStr));
                        insertStatement.setInt(5, Integer.parseInt(customerIdStr));
                        insertStatement.setInt(6, quantity);
                        insertStatement.setDouble(7, productPrice);
                        insertStatement.setDouble(8, totalSale);

                        insertStatement.addBatch();
                    } catch (Exception e) {
                        System.err.println("Error processing transaction for Order_ID: " + orderIdStr + " - " + e.getMessage());
                    }
                } else {
                    System.err.println("Skipping transaction due to missing customer or product for Order_ID: " + orderIdStr);
                }
            }

            // Execute the batch insert
            int[] results = insertStatement.executeBatch();
            System.out.println("Inserted " + results.length + " records into Sales_Fact table.");
        } catch (SQLException e) {
            //System.err.println("Failed to insert sales data: " + e.getMessage());
        }
    }

    private static void insertProducts(List<Map<String, Object>> products) throws SQLException {
        String sqlInsert = "INSERT IGNORE INTO Products_Dim (Product_ID, Product_Name, Supplier_ID, Supplier_Name, Product_Price) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement insertStatement = connection.prepareStatement(sqlInsert)) {
            for (Map<String, Object> product : products) {
                Object productIdObj = product.get("productID");
                if (productIdObj != null) {
                    int productId = Integer.parseInt(productIdObj.toString());
                    insertStatement.setInt(1, productId);
                    insertStatement.setString(2, (String) product.get("productName"));
                    insertStatement.setInt(3, Integer.parseInt(product.get("supplierID").toString())); // Assuming supplierID is available
                    insertStatement.setString(4, (String) product.get("supplierName")); // Assuming supplierName is available
                    insertStatement.setDouble(5, Double.parseDouble(product.get("productPrice").toString().replace("$", "")));
                    insertStatement.addBatch();
                } else {
                    System.err.println("Warning: Product_ID is null for product: " + product);
                }
            }
            insertStatement.executeBatch();
        } catch (SQLException e) {
            System.err.println("Failed to insert products: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Error parsing Product_ID: " + e.getMessage());
        }
    }

    private static void insertCustomers(List<Map<String, Object>> customers) throws SQLException {
        String sqlInsert = "INSERT IGNORE INTO Customers_Dim (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?)";
        try (PreparedStatement insertStatement = connection.prepareStatement(sqlInsert)) {
            for (Map<String, Object> customer : customers) {
                if (customer.get("customer_id") != null) {
                    int customerId = Integer.parseInt(customer.get("customer_id").toString());
                    insertStatement.setInt(1, customerId);
                    insertStatement.setString(2, (String) customer.get("customer_name"));
                    insertStatement.setString(3, (String) customer.get("gender"));
                    insertStatement.addBatch();
                } else {
                    System.err.println("Warning: Customer_ID is null for customer: " + customer);
                }
            }
            insertStatement.executeBatch();
        } catch (SQLException e) {
            System.err.println("Failed to insert customers: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Error parsing Customer_ID: " + e.getMessage());
        }
    }

    public static List<Map<String, Object>> loadProducts(String filePath) {
        List<Map<String, Object>> products = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            // Skip the header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length < 3) {
                    System.err.println("Invalid line: " + line);
                    continue;
                }
                Map<String, Object> product = new HashMap<>();
                product.put("productID", values[0].trim());
                product.put("productName", values[1].trim());
                try {
                    // Remove the dollar sign and parse the price
                    product.put("Product_Price", Double.parseDouble(values[2].replace("$", "").trim()));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid price for product ID: " + values[0] + " in line: " + line);
                    continue;
                }
                product.put("supplierID", values[3].trim());
                product.put("supplierName", values[4].trim());
                product.put("storeID", values[5].trim());
                product.put("storeName", values[6].trim());
                products.add(product);
            }
        } catch (Exception e) {
            System.err.println("Error loading products: " + e.getMessage());
        }
        return products;
    }

    private static void logNullPrice(String productId) {
        try (FileWriter fw = new FileWriter("null_prices.log", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println("Product ID " + productId + " has null price - " + new java.util.Date());
        } catch (IOException e) {
            System.err.println("Failed to log null price for product ID: " + productId);
        }
    }
}
