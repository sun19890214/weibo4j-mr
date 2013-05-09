package weibo4j.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;


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
  private static final Logger logger = Logger.getLogger(Topic.class);
  
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
    
    @Override
    // output key => topic, output value => topic statistics over all tweets
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      Set<CommentsOrRepostsComparable> rank = new TreeSet<CommentsOrRepostsComparable>();
      
      long total_tweets_count = 0;
      long total_comments_count = 0;
      long total_reposts_count = 0;
      long max_comments_count = 0;
      long max_reposts_count = 0;
      Status mostComments = null;
      Status mostReposts = null;
      
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
          // by default, comments_count and reposts_count account for 50% respectively
          rank.add(new CommentsOrRepostsComparable(status));
          
          int cCount = status.getCommentsCount();
          int rCount = status.getRepostsCount();
          if (cCount > max_comments_count) {
            max_comments_count = cCount;
            mostComments = status;
          }
          if (rCount > max_reposts_count) {
            max_reposts_count = rCount;
            mostReposts = status;
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
      .append(mostComments.getJSONObject().toString())
      .append("\t")
      .append(mostReposts.getJSONObject().toString());
      
       int i = 0;
       for (CommentsOrRepostsComparable statusComparable : ((TreeSet<CommentsOrRepostsComparable>) rank).descendingSet()) {
         if (i >= 10) {
           break;
         }
         builder.append("\t" + statusComparable.getStatus().getJSONObject().toString());
         i++;
       }
      
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

  private static class CommentsOrRepostsComparable implements Comparable<CommentsOrRepostsComparable> {

    private Status status = null;
    private double commentsWeight = 0.5;
    private double repostsWeight = 0.5;
    
    public CommentsOrRepostsComparable(Status status) {
      this.status = status;
    }
     
    public CommentsOrRepostsComparable(Status status, 
        double commentsWeight) {
      this.status = status;
      this.commentsWeight = commentsWeight;
      this.repostsWeight = 1 - repostsWeight;
    }
    
    @Override
    public int compareTo(CommentsOrRepostsComparable other) {
      if (null == status && null == other) {
        return 0;
      } else if (null == status) {
        return -1;
      } else if (null == other){
        return 1;
      }
      int thisCount = (int) (this.getCommentsCount() * commentsWeight + this.getRepostsCount() * repostsWeight);
      int otherCount = (int) (other.getCommentsCount() * commentsWeight + other.getRepostsCount() * repostsWeight);
     if (thisCount < otherCount) {
        return -1;
      } else if (thisCount > otherCount) {
        return 1;
      } else {
        return status.getMid().compareTo(other.getMid());
      }
    }
     
    public int getCommentsCount() {
      return status.getCommentsCount();
    }
    
    public int getRepostsCount() {
      return status.getRepostsCount();
    }
    
    public String getMid() {
      return status.getMid();
    }
    
    public Status getStatus() {
      return status;
    }

    @Override
    public int hashCode() {
      final int prime  = 31;
      int result = 1;
      result = prime * result + ((status == null) ? 0 : status.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CommentsOrRepostsComparable other = (CommentsOrRepostsComparable) obj;
      
      return this.compareTo(other) == 0 ? true : false;
    }


    
    
  }
}
