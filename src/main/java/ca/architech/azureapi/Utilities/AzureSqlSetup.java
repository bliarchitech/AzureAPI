package ca.architech.azureapi.Utilities;

import java.sql.Connection;

public interface AzureSqlSetup {

    String getConnectionString();
    Connection connectionSetup();
    void connectionClose(Connection connection);

}
