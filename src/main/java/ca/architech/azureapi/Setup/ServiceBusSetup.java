package ca.architech.azureapi.Setup;

import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

public interface ServiceBusSetup {

    ServiceBusContract ServiceBusInit();
    QueueInfo CreateServiceBusQueue(ServiceBusContract service);
    QueueInfo GetServiceBusQueue(ServiceBusContract service);
    void DeleteServiceBusQueue(ServiceBusContract service, QueueInfo queueInfo);
    TopicInfo CreateServiceBusTopic(ServiceBusContract service);
    TopicInfo GetServiceBusTopic(ServiceBusContract service);
    void DeleteServiceBusTopic(ServiceBusContract service, TopicInfo topicInfo);

}
