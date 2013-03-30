package weibo4j.app;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class DistributedCacheClass {

  public DistributedCacheClass() {
  }
  
  public Path[] getLocalCacheFiles(Configuration conf) throws IOException {
    return DistributedCache.getLocalCacheFiles(conf);
  }

  public void addCacheFile(URI uri, Configuration conf) {
    DistributedCache.addCacheFile(uri, conf);
  }
}
