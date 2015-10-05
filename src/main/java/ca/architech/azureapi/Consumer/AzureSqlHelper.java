package ca.architech.azureapi.Consumer;

import ca.architech.azureapi.Model.Temperature;

import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

public class AzureSqlHelper {

    private static final Logger logger = Logger.getLogger(AzureSqlHelper.class.getName());

    public static void createTable(Connection connection, String azureDBName) {
        try {
            String sqlString =
                            "CREATE TABLE " + azureDBName + " (" +
                            "[ID] [int] IDENTITY(1,1) NOT NULL," +
                            "[VAL] [nvarchar](50) NOT NULL," +
                            "[X] [nvarchar](50) NOT NULL," +
                            "[Y] [nvarchar](50) NOT NULL," +
                            "[Z] [nvarchar](50) NOT NULL" + ")";

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("Table " + azureDBName + " Created.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void dropTable(Connection connection, String azureDBName) {
        try {
            String sqlString = "DROP TABLE " + azureDBName;

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("Table " + azureDBName + " Dropped.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void insertData(Connection connection, String azureDBName, List<Temperature> list) {
        if (list.size() == 0) {
            logger.info("List is EMPTY!");
            return;
        }

        try {
            String sqlString =
                            "SET IDENTITY_INSERT " + azureDBName + " ON " +
                            "INSERT INTO " + azureDBName + " (ID, VAL, X, Y, Z) VALUES";

            for (int i = 0; i < list.size(); i++) {
                if (i == list.size()-1) {
                    sqlString += "(" + i+1 + ", '" + list.get(i).getValue() +
                            "', '" + list.get(i).getX() +
                            "', '" + list.get(i).getY() +
                            "', '" + list.get(i).getZ() + "')";
                }
                else {
                    sqlString += "(" + i+1 + ", '" + list.get(i).getValue() +
                            "', '" + list.get(i).getX() +
                            "', '" + list.get(i).getY() +
                            "', '" + list.get(i).getZ() + "'),";
                }
            }

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("Data Inserted.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getAllData(Connection connection, String azureDBName) {
        ResultSet resultSet = null;

        try {
            String sqlString = "SELECT * FROM " + azureDBName;

            PreparedStatement pstmt = connection.prepareStatement(sqlString);

            resultSet = pstmt.executeQuery();
            while(resultSet.next()){
                int id  = resultSet.getInt("ID");
                String value = resultSet.getString("VAL");
                String x = resultSet.getString("X");
                String y = resultSet.getString("Y");
                String z = resultSet.getString("Z");

                logger.info("ID: " + id + ", Value: " + value +
                        ", X: " + x + ", Y: " + y + ", Z: " + z);
            }

            logger.info("Get All Data.");
        }
        catch (Exception e) {
            logger.info("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != resultSet) resultSet.close();
            }
            catch (SQLException sqlException) {
                logger.warning("SQL Exception " + sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void getData(Connection connection, String azureDBName, Double xVal, Double yVal) {
        ResultSet resultSet = null;

        try {
            String sqlString = "SELECT * FROM " + azureDBName +
                    " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            PreparedStatement pstmt = connection.prepareStatement(sqlString);

            resultSet = pstmt.executeQuery();
            while(resultSet.next()){
                int id  = resultSet.getInt("ID");
                String value = resultSet.getString("VAL");
                String x = resultSet.getString("X");
                String y = resultSet.getString("Y");
                String z = resultSet.getString("Z");

                logger.info("ID: " + id + ", Value: " + value +
                        ", X: " + x + ", Y: " + y + ", Z: " + z);
            }

            logger.info("Get Data.");
        }
        catch (Exception e) {
            logger.info("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != resultSet) resultSet.close();
            }
            catch (SQLException sqlException) {
                logger.warning("SQL Exception " + sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void updateData(Connection connection, String azureDBName, Double updatedVal, Double xVal, Double yVal) {
        try {
            String sqlString = "UPDATE " + azureDBName + " SET VAL=" + updatedVal.toString() +
                            " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("Data Updated.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void deleteData(Connection connection, String azureDBName, Double xVal, Double yVal) {
        try {
            String sqlString = "DELETE FROM " + azureDBName +
                    " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("Data Deleted.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void deleteAllData(Connection connection, String azureDBName) {
        try {
            String sqlString = "DELETE FROM " + azureDBName;

            PreparedStatement pstmt = connection.prepareStatement(sqlString);
            pstmt.executeUpdate();

            logger.info("All Data Deleted.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }
}
