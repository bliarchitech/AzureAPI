package ca.architech.azureapi.Utilities;

import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

public interface ServiceBusSetup {

    ServiceBusContract ServiceBusInit();
    QueueInfo CreateServiceBusQueue(ServiceBusContract service, String queueName);
    QueueInfo GetServiceBusQueue(ServiceBusContract service, String queueName);
    void DeleteServiceBusQueue(ServiceBusContract service, QueueInfo queueInfo);
    TopicInfo CreateServiceBusTopic(ServiceBusContract service, String topicName);
    TopicInfo GetServiceBusTopic(ServiceBusContract service, String topicName);
    void DeleteServiceBusTopic(ServiceBusContract service, TopicInfo topicInfo);

}
