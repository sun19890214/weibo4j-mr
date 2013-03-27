package weibo4j.app;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import weibo4j.model.Status;
import weibo4j.util.Utils;

/*
 *  trend of topic by day
 */

public class TopicByTime implements Tool {
  private static Map<String, String> topicList = null;
  private Configuration conf;
  
  
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

    DistributedCache.addCacheFile(new URI("/home/manuzhang/topic.txt#topic.txt"), conf);

    job.waitForCompletion(true);

    return 0;
  }

  protected static class TimeMapper extends Mapper<LongWritable, Text, Text, Text> {
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    List<Status> statusList = new ArrayList<Status>();
    boolean test = false; 
    
    public TimeMapper(boolean test) {
      this.test =test;
    }
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      topicList = Utils.loadTopics(context.getConfiguration(), test);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      try {
        statusList = Utils.constructStatusList(value.toString());

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
      } catch (Exception e) {
        e.printStackTrace();
      }
    }




  }

  protected static class TimeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      int count = 0;
      for (Iterator<Text> iterator = values.iterator();iterator.hasNext();) {
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
