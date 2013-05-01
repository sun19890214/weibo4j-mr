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
import org.apache.hadoop.mapreduce.Reducer.Context;
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
import weibo4j.util.Provinces;
import weibo4j.util.Utils;

public class TopicByTimeAtPlace implements Tool {
  private Configuration conf = null;
  private static final Logger logger = Logger
      .getLogger(TopicByTimeAtPlace.class);

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(TopicByTimeAtPlace.class);
    job.setJobName("TopicByTimeAtPlace");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(TimeAtPlaceMapper.class);
    job.setReducerClass(TimeAtPlaceReducer.class);
    job.setCombinerClass(TimeAtPlaceReducer.class);

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
    TimeAtPlaceMapper.cache.addCacheFile(new URI(
        "/home/manuzhang/topic.txt#topic.txt"), conf);
    TimeAtPlaceMapper.cache.addCacheFile(new URI(
        "/home/manuzhang/provinces.json#provinces.json"), conf);
    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TopicByTimeAtPlace(), args));
  }

  protected static class TimeAtPlaceMapper extends
      Mapper<LongWritable, Text, Text, LongWritable> {
    private static SimpleDateFormat inputFormat = new SimpleDateFormat(
        "yyyy-MM");
    private static DistributedCacheClass cache = new DistributedCacheClass();
    private static Map<String, String> topicList = new HashMap<String, String>();

    public TimeAtPlaceMapper() {
      
    }
   
    protected TimeAtPlaceMapper(DistributedCacheClass dcc) {
      cache = dcc;
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Path[] localPaths = cache.getLocalCacheFiles(context.getConfiguration());
      if (null == localPaths || localPaths.length <= 1) {
        throw new FileNotFoundException(
            "Not all distributed cached files could be found");
      }
      topicList = Utils.loadTopics(localPaths[0].toString());
      Provinces.loadJSON(localPaths[1].toString());
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
        for (Status status : statusList) {
          String text = Utils.removeEol(status.getText()); // get content, and
                                                           // get rid of \t, \n
          for (String pattern : topicList.keySet()) {
            if (Pattern.compile(pattern).matcher(text).find()) {
              String time = inputFormat.format(status.getCreatedAt());
              String province = Provinces.getNameFromId(status.getUser()
                  .getProvince());
              context.write(new Text(topicList.get(pattern) + "\t" + time
                  + "\t" + province), new LongWritable(1));
            }
          }
        }
      }
    }

  }

  protected static class TimeAtPlaceReducer extends
      Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      int count = 0;
      for (Iterator<LongWritable> iterator = values.iterator(); iterator.hasNext();) {
        iterator.next();
        count++;
      }
      context.write(key, new LongWritable(count));
    }
  }
}
