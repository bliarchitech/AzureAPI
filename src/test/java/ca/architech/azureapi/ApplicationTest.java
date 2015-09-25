package ca.architech.azureapi;

import ca.architech.azureapi.Model.Temperature;
import org.junit.Assert;
import org.junit.Test;

public class ApplicationTest {

    @Test
    public void javaUnitTest() {
        // Given, When, Then
        Temperature temp = new Temperature();
        temp.setValue("7");
        Assert.assertEquals("7", temp.getValue());
        System.out.println("javaUnitTest: PASS");
    }

}
