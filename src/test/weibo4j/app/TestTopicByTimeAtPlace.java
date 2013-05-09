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
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import weibo4j.app.TopicByTimeAtPlace.TimeAtPlaceMapper;
import weibo4j.app.TopicByTimeAtPlace.TimeAtPlaceReducer;

public class TestTopicByTimeAtPlace {
  MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
  ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
  String status = null;
  
  @Before
  public void setUp() throws IOException, URISyntaxException {
    DistributedCacheClass dcc = Mockito.mock(DistributedCacheClass.class);
    when(dcc.getLocalCacheFiles(any(Configuration.class))).thenReturn(
        new Path[]{new Path("resource/test/topic_by_time.txt"),
                   new Path("resource/test/provinces.json")});
    TimeAtPlaceMapper mapper = new TimeAtPlaceMapper(dcc);
    TimeAtPlaceReducer reducer = new TimeAtPlaceReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    BufferedReader reader = new BufferedReader(new FileReader("resource/test/status_by_time.txt"));
    status = reader.readLine(); 
    reader.close();
  }


  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(status))
             // the output position should match exactly
             .withOutput(new Text("历史\t2013-03\t广东"), new LongWritable(1))
             .withOutput(new Text("女人\t2013-03\t福建"), new LongWritable(1))
             .withOutput(new Text("历史\t2013-03\t广东"), new LongWritable(1))
             .withOutput(new Text("女人\t2013-03\t广东"), new LongWritable(1))
             .runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<LongWritable> values1 = new ArrayList<LongWritable>();
    values1.add(new LongWritable(1));
    values1.add(new LongWritable(1));
    
    reduceDriver.withInput(new Text("历史\t2013-03\t广东"), values1)
                .withOutput(new Text("历史\t2013-03\t广东"), new LongWritable(2))
                .runTest();
  }
  
  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text(status));
    mapReduceDriver.addOutput(new Text("历史\t2013-03\t广东"), new LongWritable(2));
    mapReduceDriver.addOutput(new Text("女人\t2013-03\t广东"), new LongWritable(1));
    mapReduceDriver.addOutput(new Text("女人\t2013-03\t福建"), new LongWritable(1));
    mapReduceDriver.runTest();
  }

}
