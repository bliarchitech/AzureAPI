package ca.architech.azureapi.Consumer;

import ca.architech.azureapi.Model.Temperature;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class AzureSqlHelper {
    public static void createTable(Connection connection, String azureDBName) {
        Statement statement = null;

        try {
            statement = connection.createStatement();

            String sqlString =
                            "CREATE TABLE " + azureDBName +" (" +
                            "[ID] [int] IDENTITY(1,1) NOT NULL," +
                            "[VAL] [nvarchar](50) NOT NULL," +
                            "[X] [nvarchar](50) NOT NULL," +
                            "[Y] [nvarchar](50) NOT NULL," +
                            "[Z] [nvarchar](50) NOT NULL" + ")";

            statement.executeUpdate(sqlString);

            System.out.println("Table " + azureDBName + " Created.");
        }
        catch (Exception e) {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void dropTable(Connection connection, String azureDBName) {
        Statement statement = null;

        try {
            statement = connection.createStatement();

            String sqlString = "DROP TABLE " + azureDBName;

            statement.executeUpdate(sqlString);

            System.out.println("Table " + azureDBName + " Dropped.");
        }
        catch (Exception e) {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void insertData(Connection connection, String azureDBName, List<Temperature> list) {
        if (list.size() == 0) {
            System.out.println("List is EMPTY!");
            return;
        }

        Statement statement = null;

        try {
            statement = connection.createStatement();

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

            statement.executeUpdate(sqlString);

            System.out.println("Data Inserted.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void getAllData(Connection connection, String azureDBName) {
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.createStatement();

            String sqlString = "SELECT * FROM " + azureDBName;

            resultSet = statement.executeQuery(sqlString);
            while(resultSet.next()){
                int id  = resultSet.getInt("ID");
                String value = resultSet.getString("VAL");
                String x = resultSet.getString("X");
                String y = resultSet.getString("Y");
                String z = resultSet.getString("Z");

                System.out.print("ID: " + id);
                System.out.print(", Value: " + value);
                System.out.print(", X: " + x);
                System.out.print(", Y: " + y);
                System.out.println(", Z: " + z);
            }

            System.out.println("Get All Data.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
                if (null != resultSet) resultSet.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void getData(Connection connection, String azureDBName, Double xVal, Double yVal) {
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.createStatement();

            String sqlString = "SELECT * FROM " + azureDBName +
                    " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            resultSet = statement.executeQuery(sqlString);
            while(resultSet.next()){
                int id  = resultSet.getInt("ID");
                String value = resultSet.getString("VAL");
                String x = resultSet.getString("X");
                String y = resultSet.getString("Y");
                String z = resultSet.getString("Z");

                System.out.print("ID: " + id);
                System.out.print(", Value: " + value);
                System.out.print(", X: " + x);
                System.out.print(", Y: " + y);
                System.out.println(", Z: " + z);
            }

            System.out.println("Get Data.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
                if (null != resultSet) resultSet.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void updateData(Connection connection, String azureDBName, Double updatedVal, Double xVal, Double yVal) {
        Statement statement = null;

        try {
            statement = connection.createStatement();

            String sqlString = "UPDATE " + azureDBName + " SET VAL=" + updatedVal.toString() +
                            " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            statement.executeUpdate(sqlString);

            System.out.println("Data Updated.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void deleteData(Connection connection, String azureDBName, Double xVal, Double yVal) {
        Statement statement = null;

        try {
            statement = connection.createStatement();

            String sqlString = "DELETE FROM " + azureDBName +
                    " WHERE X=" + xVal.toString() + " AND Y=" + yVal.toString();

            statement.executeUpdate(sqlString);

            System.out.println("Data Deleted.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }

    public static void deleteAllData(Connection connection, String azureDBName) {
        Statement statement = null;

        try {
            statement = connection.createStatement();

            String sqlString = "DELETE FROM " + azureDBName;

            statement.executeUpdate(sqlString);

            System.out.println("All Data Deleted.");
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (null != statement) statement.close();
            }
            catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
                sqlException.printStackTrace();
            }
        }
    }
}
