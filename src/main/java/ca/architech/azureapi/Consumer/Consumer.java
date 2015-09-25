package ca.architech.azureapi.Consumer;

import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Setup.AzureSqlSetup;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Consumer {
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

                    System.out.println("Message" + data.getId() + " Dequeued.");
                }
                else {
                    break;
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
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

                    System.out.println("Message" + data.getId() + " Unsubscribed.");
                }
                else {
                    break;
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
        catch (Exception e) {
            System.out.print("Generic exception encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        return list;
    }

    public static void DeleteSubscription(ServiceBusContract service, TopicInfo topicInfo, String subscriptName) {
        try {
            service.deleteSubscription(topicInfo.getPath(), subscriptName);
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    public static void DatabaseManipulation(List<Temperature> list) {
        String azureDBName = "Temperature";

        Connection connection = AzureSqlSetup.connectionSetup();

        AzureSqlHelper.createTable(connection, azureDBName);

        AzureSqlHelper.insertData(connection, azureDBName, list);

        AzureSqlHelper.getAllData(connection, azureDBName);

        AzureSqlHelper.getData(connection, azureDBName, 0.58, 0.76);

        AzureSqlHelper.updateData(connection, azureDBName, 23.2, 0.58, 0.76);
        AzureSqlHelper.getData(connection, azureDBName, 0.58, 0.76);

        AzureSqlHelper.deleteData(connection, azureDBName, 0.58, 0.76);
        AzureSqlHelper.getAllData(connection, azureDBName);

        AzureSqlHelper.deleteAllData(connection, azureDBName);
        AzureSqlHelper.getAllData(connection, azureDBName);

        AzureSqlHelper.dropTable(connection, azureDBName);

        AzureSqlSetup.connectionClose(connection);
    }
}
