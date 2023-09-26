package org.Utility;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseComparator {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseComparator.class);

    public static void main(String[] args) throws SQLException {

        // Create connections to the two databases
        Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl", "scott", "tiger");
        Connection teradata = DriverManager.getConnection("jdbc:teradata://localhost:1025/database=mydb", "teradata", "teradata");

        // Get the list of tables in the two databases
        List<String> tables1 = getTables(oracle);
        List<String> tables2 = getTables(teradata);

        // Create a map to store the discrepancies between the two tables
        Map<String, List<String>> discrepancies = new HashMap<>();

        // Count the number of discrepancies
        int discrepancyCount = 0;

        // Compare the tables in the two databases
        for (String table : tables1) {

            // Get the results of a SELECT query on the table in the first database
            ResultSet resultSet1 = oracle.createStatement().executeQuery("SELECT * FROM " + table);

            // Get the results of a SELECT query on the table in the second database, ignoring the schema
            ResultSet resultSet2 = teradata.createStatement().executeQuery("SELECT * FROM " + table.toLowerCase());

            // Compare the two result sets
            while (resultSet1.next() && resultSet2.next()) {

                // Compare the values of each column in the two result sets
                for (int i = 1; i <= resultSet1.getMetaData().getColumnCount(); i++) {

                    if (!resultSet1.getString(i).equals(resultSet2.getString(i))) {

                        // Add the discrepancy to the discrepancies map
                        List<String> tableDiscrepancies = discrepancies.get(table);
                        if (tableDiscrepancies == null) {
                            tableDiscrepancies = new ArrayList<>();
                            discrepancies.put(table, tableDiscrepancies);
                        }
                        tableDiscrepancies.add("Column " + i + ": " + resultSet1.getString(i) + " vs. " + resultSet2.getString(i));

                        // Increment the discrepancy count
                        discrepancyCount++;

                    }

                }

            }

            // Close the result sets
            resultSet1.close();
            resultSet2.close();

        }

        // Close the database connections
        oracle.close();
        teradata.close();

        // Log the discrepancies
        for (Map.Entry<String, List<String>> entry : discrepancies.entrySet()) {
            logger.info("Table " + entry.getKey() + ": ");
            for (String discrepancy : entry.getValue()) {
                logger.info("    " + discrepancy);
            }
        }

        // Print the number of discrepancies
        System.out.println("There were " + discrepancyCount + " discrepancies found between the two databases.");

    }

    private static List<String> getTables(Connection connection) throws SQLException {

        // Create a list to store the tables in the database
        List<String> tables = new ArrayList<>();

        // Get the database metadata
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        // Get a list of all tables in the database
        ResultSet tablesResultSet = databaseMetaData.getTables(null, null, null, null);

        // Add the tables to the list
        while (tablesResultSet.next()) {
            tables.add(tablesResultSet.getString("TABLE_NAME"));
        }

        // Close the result set
        tablesResultSet.close();

        // Return the list of tables
        return tables;

    }

}
