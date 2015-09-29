package ca.architech.azureapi.Setup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class AzureSqlSetup {

    private static final Logger logger = Logger.getLogger(AzureSqlSetup.class.getName());

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
        catch (ClassNotFoundException cnfe) {
            logger.warning("ClassNotFoundException " + cnfe.getMessage());
        }
        catch (Exception e) {
            logger.warning("Exception " + e.getMessage());
            e.printStackTrace();
        }

        logger.info("Database Connected.");

        return connection;
    }

    public static void connectionClose(Connection connection) {
        try {
            if (null != connection) connection.close();
        }
        catch (SQLException sqlException) {
            logger.warning("SQL Exception " + sqlException.getMessage());
            sqlException.printStackTrace();
        }

        logger.info("Database Disconnected.");
    }
}
