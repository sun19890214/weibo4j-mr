package weibo4j.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Log implements Tool {

  private Configuration conf = null;

  private static final Logger logger = Logger.getLogger(Log.class);
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Log(), args));    
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(Log.class);
    job.setJobName("Log");

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setMapperClass(LogMapper.class);

    job.waitForCompletion(true);

    return 0;
  }

  protected static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void setup(Context context) {
      logger.info("setup");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        logger.info("map");
    }
  }


  @Override
  public Configuration getConf() {
    logger.info("get conf");
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    logger.info("set conf");
    this.conf = conf; 
  }
}