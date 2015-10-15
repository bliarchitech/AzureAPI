package ca.architech.azureapi.Utilities;

import ca.architech.azureapi.Model.Temperature;

import java.sql.Connection;
import java.util.List;

public interface AzureSqlHelper {

    void createTable(Connection connection, String azureDBName);
    void dropTable(Connection connection, String azureDBName);
    void insertData(Connection connection, String azureDBName, List<Temperature> list);
    void getAllData(Connection connection, String azureDBName);
    void getData(Connection connection, String azureDBName, Double xVal, Double yVal);
    void updateData(Connection connection, String azureDBName, Double updatedVal, Double xVal, Double yVal);
    void deleteData(Connection connection, String azureDBName, Double xVal, Double yVal);
    void deleteAllData(Connection connection, String azureDBName);

}
