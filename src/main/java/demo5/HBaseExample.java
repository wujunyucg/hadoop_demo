package demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseExample {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("peoples"));
        HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
        hcdInfo.setMaxVersions(3);
        HColumnDescriptor hcdData = new HColumnDescriptor("data");
        htd.addFamily(hcdInfo);
        htd.addFamily(hcdData);
        admin.createTable(htd);
        admin.close();

    }
}
