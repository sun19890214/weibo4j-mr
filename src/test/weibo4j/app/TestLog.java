package weibo4j.app;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import weibo4j.app.Log.LogMapper;

public class TestLog {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
  
  @Before
  public void setUp() throws IOException, URISyntaxException {
    Mapper mapper = new LogMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }


  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text());
    mapDriver.runTest();
 }

}
