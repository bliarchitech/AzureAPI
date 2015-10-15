package ca.architech.azureapi;

import ca.architech.azureapi.Controller.XmlHandler;
import ca.architech.azureapi.Model.Temperature;
import ca.architech.azureapi.Utilities.ServiceBusSetupImpl;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.List;
import java.util.logging.Logger;

public class ApplicationTest {

    private static final Logger logger = Logger.getLogger(ApplicationTest.class.getName());

    @Test
    public void XmlFileExists() {
        File xmlFile = new File("src/main/resources/assets/TemperatureData.xml");
        Assert.assertFalse(!xmlFile.exists());
        logger.info("XmlFileExists: PASS");
    }

    @Test
    public void XmlFileCorrectFormat() {
        try {
            File xmlFile = new File("src/main/resources/assets/TemperatureData.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();
            NodeList contentItemSet = doc.getElementsByTagName("collection_item");
            Assert.assertNotNull(contentItemSet);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("XmlFileCorrectFormat: PASS");
    }

    @Test
    public void TemperatureListNotEmpty() {
        List<Temperature> list = XmlHandler.TemperatureList();
        Assert.assertNotNull(list);
        logger.info("TemperatureListNotEmpty: PASS");
    }

    @Test
    public void TemperatureListNoDataMissing() {
        List<Temperature> list = XmlHandler.TemperatureList();
        for (int i = 0; i < list.size(); i++) {
            Assert.assertNotEquals(null, list.get(i).getValue());
            Assert.assertNotEquals("", list.get(i).getValue());
            Assert.assertNotEquals(null, list.get(i).getX());
            Assert.assertNotEquals("", list.get(i).getX());
            Assert.assertNotEquals(null, list.get(i).getY());
            Assert.assertNotEquals("", list.get(i).getY());
            Assert.assertNotEquals(null, list.get(i).getZ());
            Assert.assertNotEquals("", list.get(i).getZ());
        }
        logger.info("TemperatureListNoDataMissing: PASS");
    }

    @Test
    public void MoreThanTwoServiceBusQueues() {
        ServiceBusSetupImpl serviceBusImpl = new ServiceBusSetupImpl();
        ServiceBusContract service = serviceBusImpl.ServiceBusInit();
        try {
            Assert.assertFalse(service.listQueues().getItems().size() > 1);
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
        logger.info("MoreThanTwoServiceBusQueues: PASS");
    }

    @Test
    public void MoreThanTwoServiceBusTopics() {
        ServiceBusSetupImpl serviceBusImpl = new ServiceBusSetupImpl();
        ServiceBusContract service = serviceBusImpl.ServiceBusInit();
        try {
            Assert.assertFalse(service.listTopics().getItems().size() > 1);
        }
        catch (ServiceException e) {
            logger.warning("ServiceException encountered: \n" + e.getMessage());
            System.exit(-1);
        }
        logger.info("MoreThanTwoServiceBusTopics: PASS");
    }

}
