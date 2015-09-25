package ca.architech.azureapi.Producer;

import ca.architech.azureapi.Model.Temperature;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;

import java.util.List;

public class Producer {
    public static void ServiceBusEnQueue(ServiceBusContract service, QueueInfo queueInfo) {
        List<Temperature> list = XmlHandler.TemperatureList();
        try {
            for (int i = 0; i < list.size(); i++) {
                BrokeredMessage message = new BrokeredMessage("Temperature Item " + i);
                message.setProperty("ID", list.get(i).getId());
                message.setProperty("Value", list.get(i).getValue());
                message.setProperty("X", list.get(i).getX());
                message.setProperty("Y", list.get(i).getY());
                message.setProperty("Z", list.get(i).getZ());
                service.sendQueueMessage(queueInfo.getPath(), message);

                System.out.println("Message" + list.get(i).getId() + " Enqueued.");
            }
        }
        catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
