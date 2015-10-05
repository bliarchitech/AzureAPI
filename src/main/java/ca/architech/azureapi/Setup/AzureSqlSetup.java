package ca.architech.azureapi.Setup;

import java.sql.Connection;

public interface AzureSqlSetup {

    String getConnectionString();
    Connection connectionSetup();
    void connectionClose(Connection connection);

}
