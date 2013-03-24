package weibo4j.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.model.Status;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONArray;
import weibo4j.org.json.JSONException;
import weibo4j.util.Tool;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Label;
import edu.stanford.nlp.objectbank.TokenizerFactory;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.lexparser.LexicalizedParserQuery;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.trees.Dependency;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;


public class Trend {
  private static Map<String, String> topicList = new LinkedHashMap<String, String>();

  private static final Logger logger = LoggerFactory.getLogger(Topic.class);

  static {
    PropertyConfigurator.configure("log4j.properties");
  }

  public static void main(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(Topic.class);
    job.setJobName("trend");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TopicMapper.class);
    job.setReducerClass(TopicReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(16);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

  public static class TopicMapper extends Mapper<LongWritable, Text, Text, Text> {
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    List<Status> statusList = new ArrayList<Status>();
    private LexicalizedParser lp = LexicalizedParser.loadModel(
        "edu/stanford/nlp/models/lexparser/xinhuaFactoredSegmenting.ser.gz");

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      loadTopics();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      try {
        statusList = constructStatusList(value.toString());

        if (statusList != null) {
          for(Status status : statusList) {
            String text = Tool.removeEol(status.getText()); // get content, and get rid of \t, \n
            ArrayList<String> tokens = getTokens(text);
            for (String token : tokens) {
              for (String pattern : topicList.keySet()) {
                if (Pattern.matches(pattern, token)) {
                  String time = inputFormat.format(status.getCreatedAt());
                  context.write(new Text(topicList.get(pattern)), new Text(time));
                  break;
                }
              }
            }   
          }
        }      
      } catch (WeiboException e) {
        logger.error(e.getMessage());  
      } catch (JSONException e) {
        logger.error(e.getMessage());
      }

    }

    private List<Status> constructStatusList(String statuses) throws JSONException, WeiboException {
      List<Status> statusList = new ArrayList<Status>();
      JSONArray statusArray = new JSONArray(statuses);
      for (int i = 0; i < statusArray.length(); i++) {
        statusList.add(new Status(statusArray.getJSONObject(i)));
      }
      return statusList;
    }

    private void loadTopics() throws IOException {
      BufferedReader reader = new BufferedReader(new FileReader("resource/topic.txt"));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\t");
        topicList.put(parts[1], parts[0]);  
      } 
      reader.close();
    }


    private ArrayList<String> getTokens(String tweet) {    
      ArrayList<String> tokens = new ArrayList<String>();
      TreebankLanguagePack tlp = lp.getOp().langpack();
      TokenizerFactory<? extends HasWord> tokenizerFactory = tlp.getTokenizerFactory();

      Tokenizer<? extends HasWord> tokenizer = tokenizerFactory.getTokenizer(
          new StringReader(tweet));
      List<? extends HasWord> wordList = tokenizer.tokenize();
      LexicalizedParserQuery parserQuery = lp.parserQuery();
      if (parserQuery.parse(wordList)) {
        Tree parse = parserQuery.getBestParse(); 
        parse.pennPrint();
        for (Dependency<Label, Label, Object> dependency : parse.dependencies()) {
          tokens.add(dependency.dependent().value());
        }
      }

      return tokens;

    }

  }

  public static class TopicReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
     for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
       context.write(key, iterator.next());
     }
      
   }
  }


}
