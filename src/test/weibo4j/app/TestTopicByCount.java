package weibo4j.app;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import weibo4j.app.TopicByCount.TopicByCountMapper;
import weibo4j.app.TopicByCount.TopicByCountReducer;
import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

public class TestTopicByCount {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
  String status = null;
  List<Status> statusList = null;

  @Before
  public void setUp() throws IOException, URISyntaxException, JSONException, WeiboException {
    DistributedCacheClass dcc = Mockito.mock(DistributedCacheClass.class);
    when(dcc.getLocalCacheFiles(any(Configuration.class))).thenReturn(new Path[]{new Path("resource/test/topic_by_count.txt")});
    Mapper mapper = new TopicByCountMapper(dcc);
    Reducer reducer = new TopicByCountReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    BufferedReader reader = new BufferedReader(new FileReader("resource/test/status_by_count.txt"));
    status = reader.readLine(); 
    statusList = Utils.constructStatusList(status); 
    reader.close();
  }

  
  @Test
  public void testMapper() throws JSONException, WeiboException, IOException {
    mapDriver.withInput(new LongWritable(), new Text(status))
    // the output position should match exactly
    .withOutput(new Text("湖人"), new Text("45\t31"))
    .withOutput(new Text("湖人"), new Text("22\t43"))
    .withOutput(new Text("足球"), new Text("1\t5"))
    .withOutput(new Text("足球"), new Text("4\t3"))
    .runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("45\t31"));
    values.add(new Text("22\t43"));
    
    reduceDriver.withInput(new Text("湖人"), values)
    .withOutput(new Text("湖人"), new Text("2\t67\t74"))
    .runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text(status));
    mapReduceDriver.addOutput(new Text("湖人"), new Text("2\t67\t74"));
    mapReduceDriver.addOutput(new Text("足球"), new Text("2\t5\t8"));
    mapReduceDriver.runTest();
  }


}
