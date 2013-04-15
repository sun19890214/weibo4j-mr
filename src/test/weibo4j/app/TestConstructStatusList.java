package weibo4j.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

public class TestConstructStatusList {
  @Test
  public void testConstructStatusList() throws IOException, JSONException, WeiboException {
    BufferedReader reader = new BufferedReader(new FileReader("resource/status.txt"));
    String statuses = null;
    while ((statuses = reader.readLine()) != null) {      
      List<Status> statusList = Utils.constructStatusList(statuses);
      for (Status status : statusList) {
        System.out.println(status);
      }
    }
    reader.close();
  }
}
