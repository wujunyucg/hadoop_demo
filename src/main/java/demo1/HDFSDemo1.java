package demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(new URI("hdfs://123.56.15.77:9000"), configuration);
        InputStream in = fs.open(new Path("/hdfs.xml"));
        OutputStream out = new FileOutputStream("D://hdfs_demo1/hdfs-site.xml");
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
