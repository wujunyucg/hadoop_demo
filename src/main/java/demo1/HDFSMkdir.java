package demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSMkdir {
    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(URI.create("hdfs://123.56.15.77:9000"), new Configuration(), "root");
//        boolean flag = fs.mkdirs(new Path("/dirv"));
        boolean flag = fs.delete(new Path("/dirv"), true);
        System.out.println(flag);
    }
}
