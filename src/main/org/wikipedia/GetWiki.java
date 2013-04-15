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
    BufferedReader reader = new BufferedReader(new FileReader("topic.txt"));
    BufferedWriter raw = new BufferedWriter(new FileWriter("raw.txt", true));
    BufferedWriter writer = new BufferedWriter(new FileWriter("intro.txt", true));
    String line = null;
    
    Wiki wiki = new Wiki("zh.wikipedia.org");
   while ((line = reader.readLine()) != null) {
      String[] parts = line.split("\t");
      System.out.println(parts[0]);
      String content = wiki.getRenderedText(parts[0].trim());
      int start = content.indexOf(">", content.indexOf("<p")) + 1;
      int end = content.indexOf("</p>");
      content = content.substring(start, end);
      raw.write(content + "\n");
      content = content.replaceAll("<[^<>]*>", "");
      writer.write(line + "\t" + content + "\n");
      writer.flush();
      raw.flush();
     // TimeUnit.SECONDS.sleep(30);
   }
  //  System.out.println(wiki.getRenderedText("谷歌退出中国"));

    writer.close();
    reader.close();
  }

}
