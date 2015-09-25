package ca.architech.azureapi;

import ca.architech.azureapi.Consumer.Consumer;
import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Producer.Producer;
import ca.architech.azureapi.Setup.ServiceBusSetup;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;

import java.util.List;

public class Application {

    public static String queueName = "ServiceBusQueue";
    public static String topicName = "ServiceBusTopic";

    public static void main(String[] args) {
        System.out.println("Hello Azure");

        producerExecution();

        consumerExecution();
    }

    public static void producerExecution() {
        System.out.println("Executing Producer...");

        ServiceBusContract service = ServiceBusSetup.ServiceBusInit();

        QueueInfo queueInfo = null;
        try {
            if (service.listQueues().getItems().size() == 0) {
                queueInfo = ServiceBusSetup.CreateServiceBusQueue(service);
                System.out.println("New Queue Created");
            }
            else {
                queueInfo = ServiceBusSetup.GetServiceBusQueue(service);
                System.out.println("Get Existing Queue");
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        Producer.ServiceBusEnQueue(service, queueInfo);

        System.out.println("Producer Execution DONE");
    }

    public static void consumerExecution() {
        System.out.println("Executing Consumer...");

        ServiceBusContract service = ServiceBusSetup.ServiceBusInit();

        QueueInfo queueInfo = null;
        try {
            if (service.listQueues().getItems().size() == 0) {
                System.out.println("No Queue Found - Please Create New Queue");
            }
            else {
                queueInfo = ServiceBusSetup.GetServiceBusQueue(service);
                System.out.println("Queue FOUND");
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        if (queueInfo == null) {
            System.out.println("Consumer Execution DONE");
            return;
        }

        List<Temperature> receivedMessages = Consumer.ServiceBusDeQueue(service, queueInfo);

        System.out.println("Consumer Data Extraction DONE");

        ServiceBusSetup.DeleteServiceBusQueue(service, queueInfo);

        System.out.println("Queue Deleted.");

        Consumer.DatabaseManipulation(receivedMessages);

        System.out.println("Consumer Execution DONE");
    }
}
