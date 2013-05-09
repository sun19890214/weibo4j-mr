package weibo4j.app;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

import weibo4j.app.TopicByPlace.PlaceMapper;
import weibo4j.app.TopicByPlace.PlaceReducer;

public class TestTopicByPlace {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
  String status = null;
  
  @Before
  public void setUp() throws IOException {
    DistributedCacheClass dcc = Mockito.mock(DistributedCacheClass.class);
    when(dcc.getLocalCacheFiles(any(Configuration.class))).thenReturn(
        new Path[]{new Path("resource/test/topic_by_place.txt"),
                   new Path("resource/test/provinces.json")});
    Mapper mapper = new PlaceMapper(dcc);
    Reducer reducer = new PlaceReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    BufferedReader reader = new BufferedReader(new FileReader("resource/test/status_by_place.txt"));
    status = reader.readLine(); 
    reader.close();
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(status))
             // the output position should match exactly
             .withOutput(new Text("应用\t广东"), new Text("1"))
             .withOutput(new Text("快乐\t北京"), new Text("1"))
             .withOutput(new Text("应用\t湖南"), new Text("1"))
             .withOutput(new Text("快乐\t安徽"), new Text("1"))
             .runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<Text> values1 = new ArrayList<Text>();
    values1.add(new Text("1"));
    
    reduceDriver.withInput(new Text("快乐\t北京"), values1)
                .withOutput(new Text("快乐\t北京"), new Text("1"))
                .runTest();
  }
  
  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text(status));
    mapReduceDriver.addOutput(new Text("应用\t广东"), new Text("1"));
    mapReduceDriver.addOutput(new Text("应用\t湖南"), new Text("1"));
    mapReduceDriver.addOutput(new Text("快乐\t北京"), new Text("1"));
    mapReduceDriver.addOutput(new Text("快乐\t安徽"), new Text("1"));
    mapReduceDriver.runTest();
  }
}
