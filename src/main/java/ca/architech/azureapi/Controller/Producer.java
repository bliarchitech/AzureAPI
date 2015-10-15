package ca.architech.azureapi.Controller;

import ca.architech.azureapi.Model.Temperature;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.*;

import java.util.List;
import java.util.logging.Logger;

public class Producer {

    private static final Logger logger = Logger.getLogger(Producer.class.getName());

    public static void ServiceBusEnQueue(ServiceBusContract service, QueueInfo queueInfo) {
        List<Temperature> list = XmlHandler.TemperatureList();
        if (list.isEmpty()) {
            logger.warning("Temperature List is Empty");
            System.exit(-1);
        }

        try {
            for (int i = 0; i < list.size(); i++) {
                BrokeredMessage message = new BrokeredMessage("Temperature Item " + i);
                message.setProperty("ID", list.get(i).getId());
                message.setProperty("Value", list.get(i).getValue());
                message.setProperty("X", list.get(i).getX());
                message.setProperty("Y", list.get(i).getY());
                message.setProperty("Z", list.get(i).getZ());
                service.sendQueueMessage(queueInfo.getPath(), message);

                logger.info("Message" + list.get(i).getId() + " Enqueued.");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
    }

    public static void ServiceBusTopicSubscribe(ServiceBusContract service, TopicInfo topicInfo) {
        List<Temperature> list = XmlHandler.TemperatureList();
        if (list.isEmpty()) {
            logger.warning("Temperature List is Empty");
            System.exit(-1);
        }

        try {
            for (int i = 0; i < list.size(); i++) {
                BrokeredMessage message = new BrokeredMessage("Temperature Item " + i);
                message.setProperty("ID", list.get(i).getId());
                message.setProperty("Value", list.get(i).getValue());
                message.setProperty("X", list.get(i).getX());
                message.setProperty("Y", list.get(i).getY());
                message.setProperty("Z", list.get(i).getZ());
                service.sendTopicMessage(topicInfo.getPath(), message);

                logger.info("Message" + list.get(i).getId() + " Subscribed.");
            }
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
    }

    public static SubscriptionInfo CreateSubscriptionDefault(ServiceBusContract service,
                                                             TopicInfo topicInfo, String subscriptName) {
        SubscriptionInfo subscriptInfo = null;

        try {
            subscriptInfo = new SubscriptionInfo(subscriptName);
            //CreateSubscriptionResult result = service.createSubscription(topicInfo.getPath(), subscriptionInfo);
            service.createSubscription(topicInfo.getPath(), subscriptInfo);
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        return subscriptInfo;
    }

    public static SubscriptionInfo CreateSubscriptionWithRules(ServiceBusContract service, TopicInfo topicInfo,
                                                               String subscriptName, String ruleName, String ruleExpression) {
        SubscriptionInfo subscriptInfo = null;

        try {
            subscriptInfo = new SubscriptionInfo(subscriptName);
            //CreateSubscriptionResult result = service.createSubscription(topicInfo.getPath(), subscriptionInfo);
            service.createSubscription(topicInfo.getPath(), subscriptInfo);

            RuleInfo ruleInfo = new RuleInfo(ruleName);
            ruleInfo = ruleInfo.withSqlExpressionFilter(ruleExpression);
            //CreateRuleResult ruleResult = service.createRule(topicInfo.getPath(), subscriptName, ruleInfo);
            service.createRule(topicInfo.getPath(), subscriptName, ruleInfo);
            service.deleteRule(topicInfo.getPath(), subscriptName, "$Default");
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }

        return subscriptInfo;
    }

    public static Boolean isSubscriptionFound(List<SubscriptionInfo> subscriptList, String subscriptName) {
        for (int i = 0; i < subscriptList.size(); i++) {
            if (subscriptList.get(i).getName().equals(subscriptName)) {
                return true;
            }
        }
        return false;
    }
}
