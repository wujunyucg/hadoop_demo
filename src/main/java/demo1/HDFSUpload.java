package demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSUpload {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(URI.create("hdfs://123.56.15.77:9000/text.txt"), configuration, "root");
        InputStream in = new FileInputStream("D://hdfs_demo1/123.txt");
        OutputStream outputStream = fs.create(new Path("hdfs://123.56.15.77:9000/text.txt"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, outputStream, 4096, true);
    }
}
