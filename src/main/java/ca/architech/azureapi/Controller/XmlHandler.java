package ca.architech.azureapi.Controller;

import ca.architech.azureapi.Model.Temperature;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class XmlHandler {

    private static final Logger logger = Logger.getLogger(XmlHandler.class.getName());

    public static List<Temperature> TemperatureList() {
        List<Temperature> tList = new ArrayList<Temperature>();

        try {
            File xmlFile = new File("src/main/resources/assets/TemperatureData.xml");
            if (!xmlFile.exists()) {
                logger.warning("No Input Temperature File");
                System.exit(-1);
            }

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList contentItemSet = doc.getElementsByTagName("collection_item");
            if (contentItemSet.getLength() == 0) {
                logger.warning("File has incorrect format");
                System.exit(-1);
            }

            for (int i = 0; i < contentItemSet.getLength(); i++) {
                Node contentItem = contentItemSet.item(i);
                Temperature currTempData = new Temperature();

                if (contentItem.getNodeType() == Node.ELEMENT_NODE) {
                    Element contentItemElement = (Element) contentItem;
                    if (contentItemElement.getElementsByTagName("temperature_id").getLength() != 0) {
                        currTempData.setId(Integer.parseInt(contentItemElement.getElementsByTagName("temperature_id").item(0).getTextContent()));
                    }

                    if (contentItemElement.getElementsByTagName("temperature_val").getLength() != 0) {
                        currTempData.setValue(contentItemElement.getElementsByTagName("temperature_val").item(0).getTextContent());
                    }

                    if (contentItemElement.getElementsByTagName("x_coordinate").getLength() != 0) {
                        currTempData.setX(contentItemElement.getElementsByTagName("x_coordinate").item(0).getTextContent());
                    }

                    if (contentItemElement.getElementsByTagName("y_coordinate").getLength() != 0) {
                        currTempData.setY(contentItemElement.getElementsByTagName("y_coordinate").item(0).getTextContent());
                    }

                    if (contentItemElement.getElementsByTagName("z_coordinate").getLength() != 0) {
                        currTempData.setZ(contentItemElement.getElementsByTagName("z_coordinate").item(0).getTextContent());
                    }
                }

                tList.add(currTempData);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return tList;
    }
}
