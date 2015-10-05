package ca.architech.azureapi;

import ca.architech.azureapi.Consumer.Consumer;
import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Producer.Producer;
import ca.architech.azureapi.Setup.ServiceBusSetupImpl;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

import java.util.List;
import java.util.logging.Logger;

public class Application {

    public static String queueName = "ServiceBusQueue";
    public static String topicName = "ServiceBusTopic";

    private static final Logger logger = Logger.getLogger(Application.class.getName());
    private static ServiceBusSetupImpl serviceBusImpl = new ServiceBusSetupImpl();

    public static void main(String[] args) {
        ServiceBusContract service = serviceBusImpl.ServiceBusInit();

        producerQueueExecution(service);

        consumerQueueExecution(service);

        producerTopicExecution(service);

        consumerTopicExecution(service);
    }

    public static void producerQueueExecution(ServiceBusContract service) {
        logger.info("Executing Queue Producer...");

        QueueInfo queueInfo = null;
        try {
            if (service.listQueues().getItems().size() == 0) {
                queueInfo = serviceBusImpl.CreateServiceBusQueue(service);
                logger.info("New Queue Created");
            }
            else {
                queueInfo = serviceBusImpl.GetServiceBusQueue(service);
                logger.info("Get Existing Queue");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        Producer.ServiceBusEnQueue(service, queueInfo);

        logger.info("Queue Producer Execution DONE");
    }

    public static void consumerQueueExecution(ServiceBusContract service) {
        logger.info("Executing Queue Consumer...");

        QueueInfo queueInfo = null;
        try {
            if (service.listQueues().getItems().size() == 0) {
                logger.info("No Queue Found - Please Create New Queue");
            }
            else {
                queueInfo = serviceBusImpl.GetServiceBusQueue(service);
                logger.info("Queue FOUND");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        if (queueInfo == null) {
            logger.info("Queue Consumer Execution DONE");
            return;
        }

        List<Temperature> receivedMessages = Consumer.ServiceBusDeQueue(service, queueInfo);
        logger.info("Consumer Data Extraction DONE");

        serviceBusImpl.DeleteServiceBusQueue(service, queueInfo);
        logger.info("Queue Deleted.");

        Consumer.DatabaseManipulation(receivedMessages);

        logger.info("Queue Consumer Execution DONE");
    }

    public static void producerTopicExecution(ServiceBusContract service) {
        logger.info("Executing Topic Producer...");

        TopicInfo topicInfo = null;
        try {
            if (service.listTopics().getItems().size() == 0) {
                topicInfo = serviceBusImpl.CreateServiceBusTopic(service);
                logger.info("New Topic Created");
            }
            else {
                topicInfo = serviceBusImpl.GetServiceBusTopic(service);
                logger.info("Get Existing Topic");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        try {
            if (service.listSubscriptions(topicInfo.getPath()).getItems().size() == 0) {
                Producer.CreateSubscriptionDefault(service, topicInfo, "AllMessages");
                Producer.CreateSubscriptionWithRules(service, topicInfo, "HighMessages", "myRuleGT10", "ID > 1");
                Producer.CreateSubscriptionWithRules(service, topicInfo, "LowMessages", "myRuleLE10", "ID <= 1");
                logger.info("New Subscriptions Created");
            }
            else {
                List<SubscriptionInfo> subscriptList = service.listSubscriptions(topicInfo.getPath()).getItems();
                if (!Producer.isSubscriptionFound(subscriptList, "AllMessages")) {
                    Producer.CreateSubscriptionDefault(service, topicInfo, "AllMessages");
                    logger.info("New Subscription Created");
                }
                else {
                    logger.info("Subscription Exists");
                }

                if (!Producer.isSubscriptionFound(subscriptList, "HighMessages")) {
                    Producer.CreateSubscriptionWithRules(service, topicInfo, "HighMessages", "myRuleGT10", "ID > 1");
                    logger.info("New Subscription Created");
                }
                else {
                    logger.info("Subscription Exists");
                }

                if (!Producer.isSubscriptionFound(subscriptList, "LowMessages")) {
                    Producer.CreateSubscriptionWithRules(service, topicInfo, "LowMessages", "myRuleLE10", "ID <= 1");
                    logger.info("New Subscription Created");
                }
                else {
                    logger.info("Subscription Exists");
                }
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        Producer.ServiceBusTopicSubscribe(service, topicInfo);

        logger.info("Topic Producer Execution DONE");
    }

    public static void consumerTopicExecution(ServiceBusContract service) {
        logger.info("Executing Topic Consumer...");

        TopicInfo topicInfo = null;
        try {
            if (service.listTopics().getItems().size() == 0) {
                topicInfo = serviceBusImpl.CreateServiceBusTopic(service);
                logger.info("No Topic Found - Please Create New Topic");
            }
            else {
                topicInfo = serviceBusImpl.GetServiceBusTopic(service);
                logger.info("Topic FOUND");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        if (topicInfo == null) {
            logger.info("Topic Consumer Execution DONE");
            return;
        }

        List<Temperature> lowMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "LowMessages");
        logger.info("Consumer Data LowMessages Extraction DONE");

        List<Temperature> highMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "HighMessages");
        logger.info("Consumer Data HighMessages Extraction DONE");

        List<Temperature> AllMessages = Consumer.ServiceBusTopicUnSubscribe(service, topicInfo, "AllMessages");
        logger.info("Consumer Data AllMessages Extraction DONE");

        Consumer.DeleteSubscription(service, topicInfo, "LowMessages");
        Consumer.DeleteSubscription(service, topicInfo, "HighMessages");
        Consumer.DeleteSubscription(service, topicInfo, "AllMessages");
        logger.info("Subscriptions Deleted");

        serviceBusImpl.DeleteServiceBusTopic(service, topicInfo);
        logger.info("Topic Deleted");

        Consumer.DatabaseManipulation(AllMessages);

        logger.info("Topic Consumer Execution DONE");
    }
}
