package weibo4j.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import weibo4j.app.DistributedCacheClass;
import weibo4j.org.json.JSONArray;
import weibo4j.org.json.JSONException;
import weibo4j.org.json.JSONObject;

public class Provinces {

  private static final Logger LOG = Logger.getLogger(Provinces.class);

  private static Map<Integer, String> idToName = new HashMap<Integer, String>();

  DistributedCacheClass cache = new DistributedCacheClass();


  public static void loadJSON(String name) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(name)); 
    StringBuilder source = new StringBuilder();
    String line = reader.readLine();
    while (line != null) {
      source.append(line);
      line = reader.readLine();
    }
    if (source.length() != 0) {
      JSONObject json;
      try {
        json = new JSONObject(source.toString());

        JSONArray provinceArray = (JSONArray) json.get("provinces");
        for (int i = 0; i < provinceArray.length(); i++) {
          JSONObject province = (JSONObject) provinceArray.get(i);
          idToName.put(province.getInt("id"), province.getString("name"));
        }
      }  catch (JSONException e) {
        LOG.error(e.getMessage());
      }
    }
    reader.close(); 
  }

  public static String getNameFromId(int id) {
    return idToName.get(id);
  }
}
