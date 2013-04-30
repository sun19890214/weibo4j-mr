package weibo4j.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.org.json.JSONObject;



public class Extract {

	public static void main(String[] args) throws IOException, WeiboException, JSONException {
		//extractCount();
		extractPosts();
	}

	/* Input:
	 *   topic \t tweets_count \t comments_count \t reposts_count \t most_commented \t most_reposted \t top 10 posts
	 * Output:
	 *   topic \t tweets_count \t comments_count \t reposts_count
	 */

	private static void extractCount() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("resource/topic_count.txt"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("resource/topic_count_min.txt", true));
		String line = null;
		Set<String> topic1 = new HashSet<String>();
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("\t");
			// assert(parts.length == 16);
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

	/* Input:
	 *   topic \t tweets_count \t comments_count \t reposts_count \t most_commented \t most_reposted \t top 10 posts
	 * Output:
	 *   topic \t reconstructed top 10 posts
	 */
	private static void extractPosts() throws IOException, WeiboException, JSONException {
		BufferedReader reader = new BufferedReader(new FileReader("resource/topic_count.txt"));
		BufferedWriter writer = new BufferedWriter(new FileWriter("resource/topic_posts.txt"));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("\t");
			String topic = parts[0];
			for (int i = 6; i < parts.length; i++) {
				Status status = new Status(new JSONObject(parts[i]));
				Post post = new Post(
						status.getUser().getScreenName(),
						status.getCreatedAt().toString(),
						topic,
						status.getText(),
						status.getCommentsCount(),
						status.getRepostsCount());
				Gson gson = new Gson();
				writer.write(topic + "\t" + gson.toJson(post).toString() + "\n");
			}
		}
		reader.close();
		writer.close();
	}

	private static class Post {
		private String user_name = null;
		private String created_at = null;
		private String topic = null;
		private String text = null;
		private int comments_count = 0;
		private int reposts_count = 0;

		Post(String user_name, String created_at, 
				String topic, String text,
				int comments_count, int reposts_count) {
			this.user_name = user_name;
			this.created_at = created_at;
			this.topic = topic;
			this.text = text;
			this.comments_count = comments_count;
			this.reposts_count = reposts_count;
		}

	}


}
