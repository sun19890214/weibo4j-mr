package weibo4j.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONArray;
import weibo4j.org.json.JSONException;

public class TestConstructStatusList {
  @Test
  public void testConstructStatusList() throws IOException, JSONException, WeiboException {
    BufferedReader reader = new BufferedReader(new FileReader("resource/status.txt"));
    String statuses = null;
    while ((statuses = reader.readLine()) != null) {      List<Status> statusList = constructStatusList(statuses);
      for (Status status : statusList) {
        System.out.println(status);
      }
    }
    reader.close();
  }

  private List<Status> constructStatusList(String statuses) throws JSONException, WeiboException {
    List<Status> statusList = new ArrayList<Status>();
    JSONArray statusArray = new JSONArray(statuses);
    for (int i = 0; i < statusArray.length(); i++) {
      statusList.add(new Status(statusArray.getJSONObject(i)));
    }
    return statusList;
  }
}
