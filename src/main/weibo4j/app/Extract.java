package weibo4j.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/* Input:
 *   topic \t tweets_count \t comments_count \t reposts_count \t most_commented \t most_reposted \t top 10 posts
 * Output:
 *   topic \t tweets_count \t comments_count \t reposts_count
 */
public class Extract {

  public static void main(String[] args) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader("resource/topic_count.txt"));
    BufferedWriter writer = new BufferedWriter(new FileWriter("resource/topic_count_min.txt", true));
    String line = null;
    Set<String> topic1 = new HashSet<String>();
    while ((line = reader.readLine()) != null) {
      String[] parts = line.split("\t");
      assert(parts.length == 16);
      topic1.add(parts[0]);
      writer.append(parts[0] + "\t")   // topic
            .append(parts[1] + "\t")   // tweets_count
            .append(parts[2] + "\t")   // comments_count
            .append(parts[3] + "\n");  // reposts_count
    }
    writer.close();
    reader.close();
    Set<String> topic2 = new HashSet<String>();
    writer = new BufferedWriter(new FileWriter("resource/missing.txt"));
    reader = new BufferedReader(new FileReader("resource/topic.txt"));
    while ((line = reader.readLine()) != null) {
      topic2.add(line.split("\t")[0]);
    }
    
    for (String t : topic2) {
      if (!topic1.contains(t)) {
        writer.append(t + "\n");
      }
    }
    
    writer.close();
    reader.close();
  }
}
      