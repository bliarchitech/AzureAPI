package ca.architech.azureapi;

import ca.architech.azureapi.Consumer.Consumer;
import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Producer.Producer;
import ca.architech.azureapi.Setup.ServiceBusSetup;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

import java.util.List;

public class Application {

    public static String queueName = "ServiceBusQueue";
    public static String topicName = "ServiceBusTopic";

    public static void main(String[] args) {
        ServiceBusContract service = ServiceBusSetup.ServiceBusInit();

        producerQueueExecution(service);

        consumerQueueExecution(service);

        producerTopicExecution(service);

        consumerTopicExecution(service);
    }

    public static void producerQueueExecution(ServiceBusContract service) {
        System.out.println("Executing Queue Producer...");

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

        System.out.println("Queue Producer Execution DONE");
    }

    public static void consumerQueueExecution(ServiceBusContract service) {
        System.out.println("Executing Queue Consumer...");

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
            System.out.println("Queue Consumer Execution DONE");
            return;
        }

        List<Temperature> receivedMessages = Consumer.ServiceBusDeQueue(service, queueInfo);

        System.out.println("Consumer Data Extraction DONE");

        ServiceBusSetup.DeleteServiceBusQueue(service, queueInfo);

        System.out.println("Queue Deleted.");

        Consumer.DatabaseManipulation(receivedMessages);

        System.out.println("Queue Consumer Execution DONE");
    }

    public static void producerTopicExecution(ServiceBusContract service) {
        System.out.println("Executing Topic Producer...");

        TopicInfo topicInfo = null;
        try {
            if (service.listTopics().getItems().size() == 0) {
                topicInfo = ServiceBusSetup.CreateServiceBusTopic(service);
                System.out.println("New Topic Created");
            }
            else {
                topicInfo = ServiceBusSetup.GetServiceBusTopic(service);
                System.out.println("Get Existing Topic");
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        try {
            if (service.listSubscriptions(topicInfo.getPath()).getItems().size() == 0) {
                Producer.CreateSubscriptionDefault(service, topicInfo, "AllMessages");
                Producer.CreateSubscriptionWithRules(service, topicInfo, "HighMessages", "myRuleGT10", "ID > 1");
                Producer.CreateSubscriptionWithRules(service, topicInfo, "LowMessages", "myRuleLE10", "ID <= 1");
                System.out.println("New Subscriptions Created");
            }
            else {
                List<SubscriptionInfo> subscriptList = service.listSubscriptions(topicInfo.getPath()).getItems();
                if (!Producer.isSubscriptionFound(subscriptList, "AllMessages")) {
                    Producer.CreateSubscriptionDefault(service, topicInfo, "AllMessages");
                    System.out.println("New Subscription Created");
                }
                else {
                    System.out.println("Subscription Exists");
                }

                if (!Producer.isSubscriptionFound(subscriptList, "HighMessages")) {
                    Producer.CreateSubscriptionWithRules(service, topicInfo, "HighMessages", "myRuleGT10", "ID > 1");
                    System.out.println("New Subscription Created");
                }
                else {
                    System.out.println("Subscription Exists");
                }

                if (!Producer.isSubscriptionFound(subscriptList, "LowMessages")) {
                    Producer.CreateSubscriptionWithRules(service, topicInfo, "LowMessages", "myRuleLE10", "ID <= 1");
                    System.out.println("New Subscription Created");
                }
                else {
                    System.out.println("Subscription Exists");
                }
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        Producer.ServiceBusTopicSubscribe(service, topicInfo);

        System.out.println("Topic Producer Execution DONE");
    }

    public static void consumerTopicExecution(ServiceBusContract service) {
        System.out.println("Executing Topic Consumer...");

        TopicInfo topicInfo = null;
        try {
            if (service.listTopics().getItems().size() == 0) {
                topicInfo = ServiceBusSetup.CreateServiceBusTopic(service);
                System.out.println("No Topic Found - Please Create New Topic");
            }
            else {
                topicInfo = ServiceBusSetup.GetServiceBusTopic(service);
                System.out.println("Topic FOUND");
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        if (topicInfo == null) {
            System.out.println("Topic Consumer Execution DONE");
            return;
        }

        List<Temperature> lowMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "LowMessages");
        System.out.println("Consumer Data LowMessages Extraction DONE");

        List<Temperature> highMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "HighMessages");
        System.out.println("Consumer Data HighMessages Extraction DONE");

        List<Temperature> AllMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "AllMessages");
        System.out.println("Consumer Data AllMessages Extraction DONE");

        Consumer.DeleteSubscription(service, topicInfo, "LowMessages");
        Consumer.DeleteSubscription(service, topicInfo, "HighMessages");
        Consumer.DeleteSubscription(service, topicInfo, "AllMessages");
        System.out.println("Subscriptions Deleted");

        ServiceBusSetup.DeleteServiceBusTopic(service, topicInfo);
        System.out.println("Topic Deleted");

        Consumer.DatabaseManipulation(AllMessages);

        System.out.println("Topic Consumer Execution DONE");
    }
}
