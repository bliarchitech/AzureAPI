package ca.architech.azureapi.Setup;

import ca.architech.azureapi.Application;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

public class ServiceBusSetup {
    public static ServiceBusContract ServiceBusInit() {
        String azureNamespace = "bli-servicebus";
        String azureAccessKey = "RootManageSharedAccessKey";
        String azurePrimaryKey = "NTJwUWS3JAwVet0o7AMLMjj6U80QSBTSmNzakq3yVQE=";
        String azureServiceUrl = ".servicebus.windows.net";

        Configuration config =
                ServiceBusConfiguration.configureWithSASAuthentication(
                        azureNamespace,
                        azureAccessKey,
                        azurePrimaryKey,
                        azureServiceUrl
                );

        return ServiceBusService.create(config);
    }

    public static QueueInfo CreateServiceBusQueue(ServiceBusContract service) {
        QueueInfo queueInfo = new QueueInfo(Application.queueName);
        long maxSizeInMegabytes = 5120;
        queueInfo.setMaxSizeInMegabytes(maxSizeInMegabytes);

        try {
            //CreateQueueResult result = service.createQueue(queueInfo);
            service.createQueue(queueInfo);
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        return queueInfo;
    }

    public static QueueInfo GetServiceBusQueue(ServiceBusContract service) {
        QueueInfo queueInfo = null;

        try {
            queueInfo = service.getQueue(Application.queueName).getValue();
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        return queueInfo;
    }

    public static void DeleteServiceBusQueue(ServiceBusContract service, QueueInfo queueInfo) {
        try {
            service.deleteQueue(queueInfo.getPath());
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
