package weibo4j.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

import com.google.gson.Gson;


/*
 * we intend to generate statistics on topic.
 * 
 *  Input: a .txt file listing topics and their regex pattern
 *         a .txt file listing tweets in json format
 *  Output: a .csv file with the following schema
 *   topic tweet_count comment_count repost_count most_commets_mid most_reposts_mid
 * 
 */

public class Topic implements Tool {

  private Configuration conf = new Configuration();
  private static final Logger logger = LoggerFactory.getLogger(Topic.class);

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(Topic.class);
    job.setJobName("topic");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TopicMapper.class);
    job.setReducerClass(TopicReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(16);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    Path outputPath = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    conf = job.getConfiguration();
    TopicMapper.cache.addCacheFile(new URI("/home/manuzhang/topic.txt#topic.txt"), conf);

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Topic(), args));
  }

  protected static class TopicMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static DistributedCacheClass cache = new DistributedCacheClass();
    private static Map<String, String> topicList = new LinkedHashMap<String, String>();

    public static final String COUNT = "count";
    public static final String COMMENTS_COUNT = "comments_count";
    public static final String REPOSTS_COUNT = "reposts_count";
    public static final String TEXT = "text";


    public TopicMapper() {

    }

    public TopicMapper(DistributedCacheClass dcc) {
      cache = dcc;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Path[] localPaths = cache.getLocalCacheFiles(context.getConfiguration());
      if (null == localPaths || 0 == localPaths.length) {
        throw new FileNotFoundException("Distributed cached file not found");
      }
      topicList = Utils.loadTopics(localPaths[0].toString());   
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      List<Status> statusList = null;
      try {
        statusList = Utils.constructStatusList(value.toString());

      } catch (WeiboException e) {
        logger.error(e.getMessage());  
      } catch (JSONException e) {
        logger.error(e.getMessage());
      }

      if (statusList != null) {
        for(Status status : statusList) {
          String text = Utils.removeEol(status.getText()); // get content, and get rid of \t, \n
          for (String pattern : topicList.keySet()) {
            if (Pattern.compile(pattern).matcher(text).find()) {
              context.write(new Text(topicList.get(pattern)), new Text(status.getJSONObject().toString()));
              break;
            }
          }
        }   
      }      
    }


  }


  public static class TopicReducer extends Reducer<Text, Text, Text, Text> {
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    // output key => topic, output value => topic statistics over all tweets
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      long total_tweets_count = 0;
      long total_comments_count = 0;
      long total_reposts_count = 0;
      int max_comments_count = 0;
      String max_comments_mid = "";
      int max_reposts_count = 0;
      String max_reposts_mid = "";
      for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
        Status status = null;
        try {
          status = new Status(iterator.next().toString());
        } catch (JSONException e) {
          logger.error(e.getMessage());
        } catch (WeiboException e) {
          logger.error(e.getMessage());
        }

        if (status != null) {
          int cCount = status.getCommentsCount();
          int rCount = status.getRepostsCount();
          if (cCount > max_comments_count) {
            max_comments_count = cCount;
            max_comments_mid = status.getMid();
          }
          if (rCount > max_reposts_count) {
            max_reposts_count = rCount;
            max_reposts_mid = status.getMid();
          }
          total_tweets_count++;
          total_comments_count += cCount;
          total_reposts_count += rCount; 
        }
      }
      
      StringBuilder builder = new StringBuilder();
      builder.append(String.valueOf(total_tweets_count))
      .append("\t")
      .append(String.valueOf(total_comments_count))
      .append("\t")
      .append(String.valueOf(total_reposts_count))
      .append("\t")
      .append(String.valueOf(max_comments_mid))
      .append("\t")
      .append(String.valueOf(max_reposts_mid));
      context.write(key, new Text(builder.toString()));
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }




}
