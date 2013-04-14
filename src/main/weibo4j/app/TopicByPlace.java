package weibo4j.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
import org.apache.log4j.Logger;

import weibo4j.model.Status;
import weibo4j.model.User;
import weibo4j.util.Provinces;
import weibo4j.util.Utils;

/*
 * trend of topic by place (province)
 */

public class TopicByPlace implements Tool {
  private static final Logger logger = Logger.getLogger(TopicByPlace.class);
  private Configuration conf;  

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TopicByPlace(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(TopicByPlace.class);
    job.setJobName("TopicByPlace");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(PlaceMapper.class);
    job.setReducerClass(PlaceReducer.class);
    job.setCombinerClass(PlaceReducer.class);

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
    PlaceMapper.cache.addCacheFile(new URI("/home/manuzhang/topic.txt#topic.txt"), conf);
    PlaceMapper.cache.addCacheFile(new URI("/home/manuzhang/provinces.json#provinces.json"), conf);
    job.waitForCompletion(true);

    return 0;

  }

  protected static class PlaceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static DistributedCacheClass cache = new DistributedCacheClass();
    private static Map<String, String> topicList = new LinkedHashMap<String, String>();

    public PlaceMapper() {  
    }

    protected PlaceMapper(DistributedCacheClass dcc) {
      cache = dcc;
    }



    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Path[] localPaths = cache.getLocalCacheFiles(context.getConfiguration());
      if (null == localPaths || localPaths.length <= 1) {
        throw new FileNotFoundException("Not all distributed cached files could be found");
      }
      topicList = Utils.loadTopics(localPaths[0].toString());
      Provinces.loadJSON(localPaths[1].toString());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      try {
        List<Status> statusList = Utils.constructStatusList(value.toString());

        if (statusList != null) {
          for(Status status : statusList) {
            String text = Utils.removeEol(status.getText()); // get content, and get rid of \t, \n
            for (String pattern : topicList.keySet()) {
              if (Pattern.compile(pattern).matcher(text).find()) {
                User user = status.getUser();
                int province = user.getProvince();
                context.write(new Text(topicList.get(pattern) + "\t" + Provinces.getNameFromId(province)), new Text(String.valueOf(1)));
              }
            }
          }   
        }
      } catch (Exception e) {
        logger.error(e.getMessage());
      }
    }

  }

  protected static class PlaceReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
      int count = 0;
      for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
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
