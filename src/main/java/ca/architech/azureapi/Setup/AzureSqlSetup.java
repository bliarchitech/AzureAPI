package ca.architech.azureapi.Setup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AzureSqlSetup {
    public static String getConnectionString() {
        String azureSqlServer = "f2i97u0mme.database.windows.net:1433";
        String azureSqlDatabase = "bli-database";
        String azureSqlUser = "bliarchitech@f2i97u0mme";
        String azureSqlPassword = "BLi90117";
        String azureSqlEncrypt = "true";
        String azureSqlTrustServer = "false";
        String azureSqlHostName = "*.database.windows.net";
        String azureSqlTimeout = "30";

        return "jdbc:sqlserver://" + azureSqlServer + ";" +
                "database="+ azureSqlDatabase + ";" +
                "user=" + azureSqlUser + ";" +
                "password=" + azureSqlPassword + ";" +
                "encrypt=" + azureSqlEncrypt + ";" +
                "trustServerCertificate=" + azureSqlTrustServer + ";" +
                "hostNameInCertificate=" + azureSqlHostName + ";" +
                "loginTimeout=" + azureSqlTimeout + ";";
    }

    public static Connection connectionSetup() {
        String azureSqlDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        Connection connection = null;

        try {
            Class.forName(azureSqlDriver);
            connection = DriverManager.getConnection(AzureSqlSetup.getConnectionString());
        }
        catch (ClassNotFoundException cnfe)
        {
            System.out.println("ClassNotFoundException " +
                    cnfe.getMessage());
        }
        catch (Exception e)
        {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Database Connected.");

        return connection;
    }

    public static void connectionClose(Connection connection) {
        try {
            if (null != connection) connection.close();
        }
        catch (SQLException sqlException) {
            System.out.println(sqlException.getMessage());
            sqlException.printStackTrace();
        }

        System.out.println("Database Disconnected.");
    }
}
