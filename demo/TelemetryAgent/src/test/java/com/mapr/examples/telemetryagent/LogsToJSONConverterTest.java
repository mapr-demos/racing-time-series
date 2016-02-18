package com.mapr.examples.telemetryagent;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class LogsToJSONConverterTest {
    @Test
    public void testFormatJSONRecord() throws Exception {
        TelemetryProducer.LogsToJSONConverter df = new TelemetryProducer.LogsToJSONConverter("time a1 b1 a2 b2", 42);
        Assert.assertEquals(2, df.getCarsCount());

        JSONObject jsonObject = df.formatJSONRecord("0 1 2 3 4");
        Assert.assertEquals(42, jsonObject.getInt("timestamp"));
        Assert.assertEquals(0, jsonObject.getInt("racetime"));
        System.out.println(jsonObject);
        Assert.assertEquals(2, jsonObject.getJSONArray("cars").length());

        JSONObject car1 = jsonObject.getJSONArray("cars").getJSONObject(0);
        Assert.assertEquals(1, car1.getInt("id"));
        Assert.assertEquals(1, car1.getJSONObject("sensors").getInt("a"));
        Assert.assertEquals(2, car1.getJSONObject("sensors").getInt("b"));

        JSONObject car2 = jsonObject.getJSONArray("cars").getJSONObject(1);
        Assert.assertEquals(2, car2.getInt("id"));
        Assert.assertEquals(3, car2.getJSONObject("sensors").getInt("a"));
        Assert.assertEquals(4, car2.getJSONObject("sensors").getInt("b"));
    }
}
