package ca.architech.azureapi.Controller;

import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Utilities.AzureSqlHelperImpl;
import ca.architech.azureapi.Utilities.AzureSqlSetupImpl;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    public static List<Temperature> ServiceBusDeQueue(ServiceBusContract service, QueueInfo queueInfo) {
        List<Temperature> list = new ArrayList<Temperature>();

        try {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);

            while (true) {
                ReceiveQueueMessageResult resultQM = service.receiveQueueMessage(queueInfo.getPath(), opts);

                BrokeredMessage message = resultQM.getValue();
                if (message != null && message.getMessageId() != null) {
                    byte[] b = new byte[200];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    while (-1 != numRead) {
                        s = new String(b);
                        s.trim();
                        numRead = message.getBody().read(b);
                    }

                    Temperature data = new Temperature();
                    data.setId(Integer.parseInt(message.getProperty("ID").toString()));
                    data.setValue(message.getProperty("Value").toString());
                    data.setX(message.getProperty("X").toString());
                    data.setY(message.getProperty("Y").toString());
                    data.setZ(message.getProperty("Z").toString());

                    list.add(data);
                    service.deleteMessage(message);

                    logger.info("Message" + data.getId() + " Dequeued.");
                }
                else {
                    break;
                }
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            logger.warning("Generic exception encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        return list;
    }

    public static List<Temperature> ServiceBusTopicUnSubscribe(ServiceBusContract service,
                                                               TopicInfo topicInfo, String subscriptName) {
        List<Temperature> list = new ArrayList<Temperature>();

        try {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(ReceiveMode.PEEK_LOCK);

            while (true) {
                ReceiveSubscriptionMessageResult resultSubMsg = service.receiveSubscriptionMessage(
                        topicInfo.getPath(), subscriptName, opts);

                BrokeredMessage message = resultSubMsg.getValue();
                if (message != null && message.getMessageId() != null) {
                    byte[] b = new byte[200];
                    String s = null;
                    int numRead = message.getBody().read(b);
                    while (-1 != numRead) {
                        s = new String(b);
                        s = s.trim();
                        numRead = message.getBody().read(b);
                    }

                    Temperature data = new Temperature();
                    data.setId(Integer.parseInt(message.getProperty("ID").toString()));
                    data.setValue(message.getProperty("Value").toString());
                    data.setX(message.getProperty("X").toString());
                    data.setY(message.getProperty("Y").toString());
                    data.setZ(message.getProperty("Z").toString());

                    list.add(data);
                    service.deleteMessage(message);

                    logger.info("Message" + data.getId() + " Unsubscribed.");
                }
                else {
                    break;
                }
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            logger.warning("Generic exception encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        return list;
    }

    public static void DeleteSubscription(ServiceBusContract service, TopicInfo topicInfo, String subscriptName) {
        try {
            service.deleteSubscription(topicInfo.getPath(), subscriptName);
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
    }

    public static void DatabaseManipulation(List<Temperature> list) {
        String azureDBName = "Temperature";

        AzureSqlSetupImpl azureSqlImpl = new AzureSqlSetupImpl();

        Connection connection = azureSqlImpl.connectionSetup();

        AzureSqlHelperImpl sqlExecution = new AzureSqlHelperImpl();

        sqlExecution.createTable(connection, azureDBName);

        sqlExecution.insertData(connection, azureDBName, list);

        sqlExecution.getAllData(connection, azureDBName);

        sqlExecution.getData(connection, azureDBName, 0.58, 0.76);

        sqlExecution.updateData(connection, azureDBName, 23.2, 0.58, 0.76);
        sqlExecution.getData(connection, azureDBName, 0.58, 0.76);

        sqlExecution.deleteData(connection, azureDBName, 0.58, 0.76);
        sqlExecution.getAllData(connection, azureDBName);

        sqlExecution.deleteAllData(connection, azureDBName);
        sqlExecution.getAllData(connection, azureDBName);

        sqlExecution.dropTable(connection, azureDBName);

        azureSqlImpl.connectionClose(connection);
    }
}
