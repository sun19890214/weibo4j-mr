package org.wikipedia;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class GetWiki {

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    BufferedReader reader = new BufferedReader(new FileReader("resource/topic_mod.txt"));
    BufferedWriter raw = new BufferedWriter(new FileWriter("resource/raw.txt", true));
    BufferedWriter writer = new BufferedWriter(new FileWriter("resource/intro.txt", true));
    String line = null;
    
    Wiki wiki = new Wiki("zh.wikipedia.org");
   while ((line = reader.readLine()) != null) {
      String[] parts = line.split("\t");
      String topic = parts[0];
      String keyword = parts[1];      
      System.out.println(topic + "\t" + keyword);
      String content = wiki.getRenderedText(keyword.trim());
      int start = content.indexOf(">", content.indexOf("<p")) + 1;
      int end = content.indexOf("</p>");
      content = content.substring(start, end);
      raw.write(content + "\n");
      content = content.replaceAll("<[^<>]*>", "")
    		           .replaceAll("\\[\\d+\\]", "");
      writer.write(topic + "\t" + content + "\n");
      writer.flush();
      raw.flush();
   }
  //  System.out.println(wiki.getRenderedText("谷歌退出中国"));
    raw.close();
    writer.close();
    reader.close();
  }

}
