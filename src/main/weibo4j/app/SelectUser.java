package weibo4j.app;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import weibo4j.util.Utils;

import weibo4j.model.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

public class SelectUser {
  public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text> {
    private Text x = new Text();
    private Text y = new Text();
    ArrayList<String> tempList = new ArrayList<String>();
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    List<Status> statusList = new ArrayList<Status>();
    Status rtStatus = null;
    String endTime = null;
    String time = null;
    String text = null;
    String rtText = null;
    ArrayList<String> keywordList = new ArrayList<String>();
    private static String separator = "|#|";

    public void configure(JobConf job) 
    {
      Calendar c=Calendar.getInstance(); 
      c.set(2013,01,01,00,00,00);
      endTime = inputFormat.format(c.getTime());
      FileInputStream fis;
      InputStreamReader isr;
      BufferedReader br;
      String line = null;
      try {
        Path[] file = DistributedCache.getLocalCacheFiles(job);
        fis = new FileInputStream(file[0].toString());
        isr = new InputStreamReader(fis,"utf8");
        br = new BufferedReader(isr);
        while((line = br.readLine()) != null)
        {
          keywordList.add(line);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      try {
        statusList.clear();
        if(value.toString().startsWith("{")) 
        {
          statusList = Utils.constructStatusList(value.toString());
        }
        if(value.toString().startsWith("["))
        {
          statusList = Utils.constructStatusList(value.toString());
        }
        if(statusList == null)
        {
          return;
        }
        for(Status status : statusList)
        {
          time = inputFormat.format(status.getCreatedAt());
          if(time.compareTo(endTime)<0)
          {
            if(status.getUser()!=null)
            {
              if(status.getUser().getFollowersCount()>=1000)
              {
                text = status.getText();
                text = Utils.removeEol(text);
                if(status.getRetweetedStatus()!=null)
                {
                  rtText = status.getRetweetedStatus().getText();
                  rtText = Utils.removeEol(rtText);
                }else
                {
                  rtText = "";
                }
                for(String keyword:keywordList)
                {
                  String[] list = keyword.split("\t");
                  String find = list[1];                      
                  Pattern p = Pattern.compile(find);
                  Matcher matcher = p.matcher(text+rtText);
                  if(matcher.find())
                  {
                    x.set(status.getUser().getId()+"\t"+list[0]);
                    y.set(1+"");
                    output.collect(x, y);
                  }
                }
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
    int count = 0;
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
      count = 0;
      while (values.hasNext()) 
      {
        ++count;
        values.next();
      }
      output.collect(key, new Text(count+""));
    }
  }
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(SelectUser.class);
    conf.setJobName("User+Event+Count");

    DistributedCache.addCacheFile(new URI("/home/haixinma/weibo/political.txt#political.txt"),conf);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setNumReduceTasks(50);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(output)){
      fs.delete(output,true);
    }
    FileOutputFormat.setOutputPath(conf, output);
    JobClient.runJob(conf);
  }


}
