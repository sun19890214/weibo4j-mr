package weibo4j.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.stanford.nlp.ling.BasicDocument;
import edu.stanford.nlp.ling.Document;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Label;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.objectbank.TokenizerFactory;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.lexparser.LexicalizedParserQuery;
import edu.stanford.nlp.parser.lexparser.Options;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.trees.Dependency;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;

public class TestTokenization {
  
  @Test
  public void testTokenization() throws IOException {
    
  /*  LexicalizedParser lp = LexicalizedParser.loadModel(
        "edu/stanford/nlp/models/lexparser/xinhuaFactoredSegmenting.ser.gz",
        new Options(),
        "-MAX_ITEMS",
        "10000000");
*/      
    LexicalizedParser lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/xinhuaFactoredSegmenting.ser.gz");
    demoDP(lp, "resourse/ text");  
  }

  private void demoDP(LexicalizedParser lp, String filename) throws IOException {
    TreebankLanguagePack tlp = lp.getOp().langpack();
    TokenizerFactory<? extends HasWord> tokenizerFactory = tlp.getTokenizerFactory();
    Document<Object, Word, Word> doc = new BasicDocument<Object>(
        (TokenizerFactory<Word>)tokenizerFactory).init(
            new InputStreamReader(new FileInputStream(filename), "UTF-8"));
    StringBuilder docStr = new StringBuilder();
    for (Iterator<?> it = doc.iterator(); it.hasNext(); ) {
      if (docStr.length() > 0) {
        docStr.append(' ');
      }
      docStr.append(it.next().toString());
    }
    Tokenizer<? extends HasWord> toke = tokenizerFactory.getTokenizer(
        new StringReader(docStr.toString()));
    List<? extends HasWord> wordList = toke.tokenize();
    LexicalizedParserQuery parserQuery = lp.parserQuery();
    if (parserQuery.parse(wordList)) {
      Tree parse = parserQuery.getBestParse(); 
      parse.pennPrint();
      Assert.assertTrue(parse.dependencies().size() > 1);
      for (Dependency<Label, Label, Object> dependency : parse.dependencies()) {
        String value = dependency.dependent().value();
        Assert.assertNotNull(value);
        System.out.println(value);
      }
    }
  }

}
