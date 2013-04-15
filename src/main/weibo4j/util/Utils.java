package weibo4j.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONArray;
import weibo4j.org.json.JSONException;
import weibo4j.org.json.JSONObject;

public class Utils {
  public static final String[] EMOTIONS = {"快乐", "悲伤", "愤怒", "恐惧", "惊奇", "厌恶", "其他"};

  public static Map<String, String> loadTopics(String path) throws IOException {
    Map<String, String> topicList = new LinkedHashMap<String, String>();
    BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split("\t");
      topicList.put(parts[1], parts[0]); 
    } 
    reader.close();
    return topicList;
  }

  public static Map<String, String> loadEmotions(String path) throws IOException {
    Map<String, String> emotionList = new LinkedHashMap<String, String>();
    BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split(",");
      assert(parts.length == EMOTIONS.length);
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].length() != 0) {
          emotionList.put(parts[i], EMOTIONS[i]);
        }
      }

    }

    reader.close();
    return emotionList;
  }

  public static List<Status> constructStatusList(String statuses) throws JSONException, WeiboException {
    List<Status> statusList = new ArrayList<Status>();
    JSONArray statusArray = new JSONArray();
    // JSONObject
    if (statuses.startsWith("{")) {
      JSONObject statusObject = new JSONObject(statuses);
      statusArray = statusObject.getJSONArray("statuses"); 
    } else if (statuses.startsWith("[")) {
      statusArray = new JSONArray(statuses);
    } else {
      throw new JSONException("Neither JSONObject nor JSONArray");
    }
    for (int i = 0; i < statusArray.length(); i++) {
      statusList.add(new Status(statusArray.getJSONObject(i)));
    }
    return statusList;
  }


  public static boolean refreshToken()
  {
    GetAccessToken wc = new GetAccessToken();      
    try {
      wc.run();
      System.out.println("refresh token finished");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      //e.printStackTrace();
      return false;
    } catch (ParseException e) {
      return false;
      // TODO Auto-generated catch block
      //e.printStackTrace();
    }
    return true;
  }

  public static boolean write(String filepath,String str)
  {
    OutputStreamWriter osw = null;
    FileOutputStream fileos = null;
    BufferedWriter bw = null;
    try
    {
      fileos = new FileOutputStream(filepath, true);
      osw = new OutputStreamWriter(fileos,"GBK");
      bw = new BufferedWriter(osw);
      if(!str.equals(""))
      {
        bw.append(str);
        bw.newLine();
      }
      bw.close();
      osw.close();
      fileos.close();
      return true;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    } 
  }
  public static boolean write(String filepath, List<String> list)
  {
    OutputStreamWriter osw = null;
    FileOutputStream fileos = null;
    BufferedWriter bw = null;
    try
    {
      fileos = new FileOutputStream(filepath, true);
      osw = new OutputStreamWriter(fileos,"GBK");
      bw = new BufferedWriter(osw);
      for (String s : list)
      {
        if(!s.equals(""))
        {
          bw.append(s);
          bw.newLine();
        }
      }
      bw.close();
      osw.close();
      fileos.close();
      return true;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
  }
  public static boolean write(String filepath, List<String> list,boolean isAppend,String encode)
  {
    OutputStreamWriter osw = null;
    FileOutputStream fileos = null;
    BufferedWriter bw = null;
    try
    {
      fileos = new FileOutputStream(filepath, isAppend);
      osw = new OutputStreamWriter(fileos,encode);
      bw = new BufferedWriter(osw);
      for (String s : list)
      {
        if(!s.equals(""))
        {
          bw.append(s);
          bw.newLine();
        }
      }
      bw.close();
      osw.close();
      fileos.close();
      return true;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
  }
  public static boolean write(String filepath, Map resultMap)
  {
    Iterator iterator = resultMap.entrySet().iterator();
    ArrayList<String> recordList = new ArrayList<String>();
    List<String> resultList = new ArrayList<String>();
    while (iterator.hasNext())
    {
      Map.Entry entry = (Map.Entry<String,Long>) iterator.next();
      resultList.add(entry.getKey()+"\t"+entry.getValue());
      //write(".\\data\\twitterHashtag",key+"\t"+value);
    }
    OutputStreamWriter osw = null;
    FileOutputStream fileos = null;
    BufferedWriter bw = null;
    try
    {
      fileos = new FileOutputStream(filepath, true);
      osw = new OutputStreamWriter(fileos,"GBK");
      bw = new BufferedWriter(osw);
      for (String s : resultList)
      {
        if(!s.equals(""))
        {
          bw.append(s);
          bw.newLine();
        }
      }
      bw.close();
      osw.close();
      fileos.close();
      return true;
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
  }
  public static String removeEol(String text)
  {
    if(text == null)
    {
      return text;
    }
    if(text.contains("\n"))
    {
      text = text.replaceAll("\n", "");
    }
    if(text.contains("\r"))
    {
      text = text.replaceAll("\r", "");
    }
    if(text.contains("\n\r"))
    {
      text = text.replaceAll("\n", "\n\r");
    }
    if(text.contains("\r\n"))
    {
      text = text.replaceAll("\r\n", "");
    }
    return text;
  }
  public static boolean deleteFile(File file)
  {
    try
    {
      if (file.exists())
      {
        file.delete();
        System.out.println("delete" +file.getName());
        return true;
      }
    }
    catch (Exception e)
    {
      System.out.println("delete failed" + file.getName());
      e.printStackTrace();
    }
    return false;
  }
}
