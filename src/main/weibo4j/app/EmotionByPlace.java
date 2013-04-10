package weibo4j.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import weibo4j.util.Provinces;
import weibo4j.util.Utils;

public class EmotionByPlace implements Tool {
  private static final Logger logger = Logger.getLogger(EmotionByPlace.class);
  private Configuration conf = null;


  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Emotion(), args));
  }



  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(Emotion.class);
    job.setJobName("EmotionByPlace");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(EmotionByPlaceMapper.class);
    job.setReducerClass(EmotionByPlaceReducer.class);

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
    EmotionByPlaceMapper.cache.addCacheFile(new URI("/home/manuzhang/emotions.txt#emotions.txt"), conf);

    job.waitForCompletion(true);

    return 0;
  }

  protected static class EmotionByPlaceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static DistributedCacheClass cache = new DistributedCacheClass();
    private Map<String, String> emotionList = null;

    public EmotionByPlaceMapper() {
    }

    protected EmotionByPlaceMapper(DistributedCacheClass dcc) {
      cache = dcc;
    }

    @Override
    public void setup(Context context) throws IOException {
      Path[] localPaths = cache.getLocalCacheFiles(context.getConfiguration());
      if (null == localPaths || 0 == localPaths.length) {
        throw new FileNotFoundException("Distributed cached file not found");
      }

      emotionList = Utils.loadEmotions(localPaths[0].toString());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
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
          String province = Provinces.getNameFromId(status.getUser().getProvince());
          for (String keyword : emotionList.keySet()) {
            int index = text.indexOf(keyword);
            while (index != -1) {
              context.write(new Text(keyword + "\t" + province), new LongWritable(1));
              text = text.substring(index + 1);
              index = text.indexOf(keyword);
            }
          }
        }
      }
    }

  }

  protected static class EmotionByPlaceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      long count = 0;
      for (Iterator<LongWritable> iterator = values.iterator(); iterator.hasNext(); ) {
        count++;
        iterator.next();
      }
      context.write(new Text(key), new LongWritable(count));
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
