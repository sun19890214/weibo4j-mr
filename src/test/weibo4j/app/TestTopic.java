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

import weibo4j.app.TopicProof.TopicProofMapper;
import weibo4j.app.TopicProof.TopicProofReducer;
import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

/* refer to TopicProof.java */

public class TestTopic {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
  String status = null;
  List<Status> statusList = null;

  @Before
  public void setUp() throws IOException, URISyntaxException, JSONException, WeiboException {
    DistributedCacheClass dcc = Mockito.mock(DistributedCacheClass.class);
    when(dcc.getLocalCacheFiles(any(Configuration.class))).thenReturn(new Path[]{new Path("resource/test/topic_by_count.txt")});
    Mapper mapper = new TopicProofMapper(dcc);
    Reducer reducer = new TopicProofReducer();
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
    String json0 = statusList.get(0).getJSONObject().toString();
    String json7 = statusList.get(7).getJSONObject().toString();
    String json8 = statusList.get(8).getJSONObject().toString();
    String json9 = statusList.get(9).getJSONObject().toString();

    mapDriver.withInput(new LongWritable(), new Text(status))
    // the output position should match exactly
    .withOutput(new Text("湖人"), new Text(json0))
    .withOutput(new Text("湖人"), new Text(json7))
    .withOutput(new Text("足球"), new Text(json8))
    .withOutput(new Text("足球"), new Text(json9))
    .runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text(statusList.get(0).getJSONObject().toString()));
    values.add(new Text(statusList.get(7).getJSONObject().toString()));
    
    reduceDriver.withInput(new Text("湖人"), values)
    .withOutput(new Text("湖人"), new Text("2\t67\t74\t"
        + statusList.get(0).getText() + "\t"
        + statusList.get(7).getText() + "\t"
        + statusList.get(0).getText() + "\t"
        + statusList.get(7).getText() 
        ))
    .runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text(status));
    mapReduceDriver.addOutput(new Text("湖人"), new Text("2\t67\t74\t"
        + statusList.get(0).getText() + "\t"
        + statusList.get(7).getText() + "\t"
        + statusList.get(0).getText() + "\t"
        + statusList.get(7).getText()
        ));
    mapReduceDriver.addOutput(new Text("足球"), new Text("2\t5\t8\t"
        + statusList.get(9).getText() + "\t"
        + statusList.get(8).getText() + "\t"
        + statusList.get(8).getText() + "\t"
        + statusList.get(9).getText()
        ));
    mapReduceDriver.runTest();
  }



}
