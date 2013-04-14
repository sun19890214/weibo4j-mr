package weibo4j.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.log4j.Logger;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

/*
 *  trend of topic by day
 */

public class TopicByTime implements Tool {
  private static final Logger logger = Logger.getLogger(TopicByTime.class);

  private Configuration conf = null;

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TopicByTime(), args));    
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(TopicByTime.class);
    job.setJobName("TopicByTime");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TimeMapper.class);
    job.setReducerClass(TimeReducer.class);
    job.setCombinerClass(TimeReducer.class);

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
    TimeMapper.cache.addCacheFile(new URI("/home/manuzhang/topic.txt#topic.txt"), conf);

    job.waitForCompletion(true);

    return 0;
  }

  protected static class TimeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DistributedCacheClass cache = new DistributedCacheClass();
    private static Map<String, String> topicList = new HashMap<String, String>();

    public TimeMapper() {  
    }

    protected TimeMapper(DistributedCacheClass dcc) {
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
      } catch (JSONException e) {
        logger.error(e.getMessage());
      } catch (WeiboException e) {
        logger.error(e.getMessage());
      }

      if (statusList != null) {
        for(Status status : statusList) {
          String text = Utils.removeEol(status.getText()); // get content, and get rid of \t, \n
          for (String pattern : topicList.keySet()) {
            if (Pattern.compile(pattern).matcher(text).find()) {
              String time = inputFormat.format(status.getCreatedAt());
              context.write(new Text(topicList.get(pattern) + "\t" + time), new Text(String.valueOf(1)));
            }
          }
        }   
      }
    } 
  }

  protected static class TimeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      int count = 0;
      for (Iterator<Text> iterator = values.iterator();iterator.hasNext();) {
        iterator.next();
        count++;
      }
      context.write(key, new Text(String.valueOf(count)));
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
