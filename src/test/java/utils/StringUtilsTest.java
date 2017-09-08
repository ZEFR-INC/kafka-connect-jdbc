package utils;

import io.confluent.connect.jdbc.util.StringUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class StringUtilsTest {

    @Test
    public void testStringToMapTwoPair() {
      String inputString = "a=1,b=2";
      Map<String, String> expected = new HashMap<>();
      expected.put("a", "1");
      expected.put("b", "2");
      Map<String, String> result = StringUtils.stringToMap(inputString, ",", "=");
      assertEquals(expected, result);
    }

    @Test
    public void testStringToMapOnePair() {
      String inputString = "a=1";
      Map<String, String> expected = new HashMap<>();
      expected.put("a", "1");
      Map<String, String> result = StringUtils.stringToMap(inputString, ",", "=");
      assertEquals(expected, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringToMapIncompletePair() {
      String inputString = "abc";
      Map<String, String> expected = new HashMap<>();
      Map<String, String> result = StringUtils.stringToMap(inputString, ",", "=");
      assertEquals(expected, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringToMapInvalidPair() {
      String inputString = "a=1=first";
      Map<String, String> expected = new HashMap<>();
      Map<String, String> result = StringUtils.stringToMap(inputString, ",", "=");
      assertEquals(expected, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringToMapNoPair() {
      String inputString = "";
      Map<String, String> expected = new HashMap<>();
      Map<String, String> result = StringUtils.stringToMap(inputString, ",", "=");
      assertEquals(expected, result);
    }
}
