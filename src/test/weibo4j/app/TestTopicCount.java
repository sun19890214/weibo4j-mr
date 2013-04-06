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

import weibo4j.app.Topic.TopicMapper;
import weibo4j.app.Topic.TopicReducer;
import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONException;
import weibo4j.util.Utils;

import com.google.gson.Gson;

public class TestTopicCount {
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
  String status = null;
  List<Status> statusList = null;

  @Before
  public void setUp() throws IOException, URISyntaxException, JSONException, WeiboException {
    DistributedCacheClass dcc = Mockito.mock(DistributedCacheClass.class);
    when(dcc.getLocalCacheFiles(any(Configuration.class))).thenReturn(new Path[]{new Path("resource/test/topic_by_count.txt")});
    Mapper mapper = new TopicMapper(dcc);
    Reducer reducer = new TopicReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    BufferedReader reader = new BufferedReader(new FileReader("resource/test/status_by_count.txt"));
    status = reader.readLine(); 
    statusList = Utils.constructStatusList(status); 
    reader.close();
  }


  @Test
  public void testMapper() throws JSONException, WeiboException {
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
  public void testReducer() {
    int[] indices = {0, 7};
    List<Text> values = new ArrayList<Text>();
    for (int index : indices) {
      values.add(new Text(statusList.get(index).getJSONObject().toString()));
    }

    reduceDriver.withInput(new Text("湖人"), values)
    .withOutput(new Text("湖人"), new Text(produceOutput(indices)))
    .runTest();
  }

  @Test
  public void testMapReduce() {
    mapReduceDriver.withInput(new LongWritable(1), new Text(status));
    mapReduceDriver.addOutput(new Text("湖人"), new Text(produceOutput(new int[]{0, 7})));
    mapReduceDriver.addOutput(new Text("足球"), new Text(produceOutput(new int[]{8, 9})));
    mapReduceDriver.runTest();
  }

  private String produceOutput(int[] indices) {
    long totalComments = 0;
    long totalReposts = 0;

    String maxCommentsMid = null;
    String maxRepostsMid = null;

    long maxComments = 0;
    long maxReposts = 0;

    for (int index : indices) {
      Status status = statusList.get(index);
      int comments = status.getCommentsCount();
      int reposts = status.getRepostsCount();
      String mid = status.getMid();

      totalComments += comments;
      totalReposts += reposts;

      if (comments > maxComments) {
        maxComments = comments;
        maxCommentsMid = mid; 
      }

      if (reposts > maxReposts) {
        maxReposts = reposts;
        maxRepostsMid = mid;
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append(indices.length + "\t");
    sb.append(String.valueOf(totalComments) + "\t");
    sb.append(String.valueOf(totalReposts) + "\t");
    sb.append(maxCommentsMid + "\t");
    sb.append(maxRepostsMid);
    return sb.toString();
  }


}
