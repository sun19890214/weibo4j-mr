package weibo4j.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

public class TestReadTopic {
  @Test
  public void testReadTopic() throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader("resource/topic.txt"));
    String line = null;
    Map<String, String> topicList = new LinkedHashMap<String, String>();
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split("\t");
      Assert.assertEquals(parts.length, 2);
      Assert.assertNotNull(parts[0]);
      Assert.assertNotNull(parts[1]);
      topicList.put(parts[1], parts[0]);  
    }
    String tweet = "百度大战360";
    String topic = "3B大战";
    boolean find = false;
    for (String regex : topicList.keySet()) {
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(tweet);
      if (matcher.find()) {
       find = true;
       Assert.assertEquals(topic, topicList.get(regex));
      }
    }
    Assert.assertTrue(find);
  }
}
