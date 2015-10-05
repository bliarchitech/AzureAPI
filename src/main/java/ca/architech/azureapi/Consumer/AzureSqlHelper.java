package ca.architech.azureapi.Consumer;

import ca.architech.azureapi.Model.Temperature;

import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

public class AzureSqlHelper {

    private static final Logger logger = Logger.getLogger(AzureSqlHelper.class.getName());

    public static void createTable(Connection connection, String azureDBName) {
        try {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("CREATE TABLE ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" ([ID] [int] IDENTITY(1,1) NOT NULL,");
            sqlBuilder.append("[VAL] [nvarchar](50) NOT NULL,");
            sqlBuilder.append("[X] [nvarchar](50) NOT NULL,");
            sqlBuilder.append("[Y] [nvarchar](50) NOT NULL,");
            sqlBuilder.append("[Z] [nvarchar](50) NOT NULL)");

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DROP TABLE ");
            sqlBuilder.append(azureDBName);

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SET IDENTITY_INSERT ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" ON ");
            sqlBuilder.append("INSERT INTO ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" (ID, VAL, X, Y, Z) VALUES");

            for (int i = 0; i < list.size(); i++) {
                sqlBuilder.append("(");
                sqlBuilder.append(i+1);
                sqlBuilder.append(", '");
                sqlBuilder.append(list.get(i).getValue());
                sqlBuilder.append("', '");
                sqlBuilder.append(list.get(i).getX());
                sqlBuilder.append("', '");
                sqlBuilder.append(list.get(i).getY());
                sqlBuilder.append("', '");
                sqlBuilder.append(list.get(i).getZ());

                if (i == list.size()-1) {
                    sqlBuilder.append("')");
                }
                else {
                    sqlBuilder.append("'),");
                }
            }

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT * FROM ");
            sqlBuilder.append(azureDBName);

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());

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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT * FROM ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" WHERE X=");
            sqlBuilder.append(xVal.toString());
            sqlBuilder.append(" AND Y=");
            sqlBuilder.append(yVal.toString());

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());

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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("UPDATE ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" SET VAL=");
            sqlBuilder.append(updatedVal.toString());
            sqlBuilder.append(" WHERE X=");
            sqlBuilder.append(xVal.toString());
            sqlBuilder.append(" AND Y=");
            sqlBuilder.append(yVal.toString());

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            sqlBuilder.append(azureDBName);
            sqlBuilder.append(" WHERE X=");
            sqlBuilder.append(xVal.toString());
            sqlBuilder.append(" AND Y=");
            sqlBuilder.append(yVal.toString());

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
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
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ");
            sqlBuilder.append(azureDBName);

            PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString());
            pstmt.executeUpdate();

            logger.info("All Data Deleted.");
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }
    }
}
