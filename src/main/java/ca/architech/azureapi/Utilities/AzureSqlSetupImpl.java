package ca.architech.azureapi.Utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class AzureSqlSetupImpl implements AzureSqlSetup {

    private static final Logger logger = Logger.getLogger(AzureSqlSetupImpl.class.getName());

    @Override
    public String getConnectionString() {
        String azureSqlServer = "f2i97u0mme.database.windows.net:1433";
        String azureSqlDatabase = "bli-database";
        String azureSqlUser = "bliarchitech@f2i97u0mme";
        String azureSqlPassword = "BLi90117";
        String azureSqlEncrypt = "true";
        String azureSqlTrustServer = "false";
        String azureSqlHostName = "*.database.windows.net";
        String azureSqlTimeout = "30";

        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:sqlserver://");
        sb.append(azureSqlServer);
        sb.append(";");
        sb.append("database=");
        sb.append(azureSqlDatabase);
        sb.append(";");
        sb.append("user=");
        sb.append(azureSqlUser);
        sb.append(";");
        sb.append("password=");
        sb.append(azureSqlPassword);
        sb.append(";");
        sb.append("encrypt=");
        sb.append(azureSqlEncrypt);
        sb.append(";");
        sb.append("trustServerCertificate=");
        sb.append(azureSqlTrustServer);
        sb.append(";");
        sb.append("hostNameInCertificate=");
        sb.append(azureSqlHostName);
        sb.append(";");
        sb.append("loginTimeout=");
        sb.append(azureSqlTimeout);
        sb.append(";");

        return sb.toString();
    }

    @Override
    public Connection connectionSetup() {
        String azureSqlDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        Connection connection = null;

        try {
            Class.forName(azureSqlDriver);
            connection = DriverManager.getConnection(getConnectionString());
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

    @Override
    public void connectionClose(Connection connection) {
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
