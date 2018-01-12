package demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableCURDJunit {
    private Configuration configuration = null;
    @Before
    public void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
    }
    @Test
    public void testPut() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTable table = new HTable(configuration, TableName.valueOf("peoples"));
        Put put = new Put(Bytes.toBytes("rk0001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhanshan"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(800000));
        table.put(put);
        table.close();
    }

    @Test
    public void testPutAll() throws IOException {
        HTable table = new HTable(configuration, TableName.valueOf("peoples"));
        List<Put> puts = new ArrayList<Put>(10000);
        for (int i = 1; i < 1000000; i++) {
            Put put = new Put(Bytes.toBytes("rk" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes("" + i));
            puts.add(put);
            if (i % 10000 == 0) {
                table.put(puts);
                puts.clear();
            }
        }
        table.put(puts);
        table.close();
    }

    @Test
    public void testGet() throws IOException {
        HTable table = new HTable(configuration, TableName.valueOf("peoples"));
        Get get = new Get(Bytes.toBytes("rk99999"));
        Result result = table.get(get);
        String string = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
        System.out.println(string);
        table.close();
    }
    @Test
    public void testScan() throws IOException {
        HTable table = new HTable(configuration, TableName.valueOf("peoples"));
        Scan scan = new Scan(Bytes.toBytes("rk29990"), Bytes.toBytes("rk30000"));
        ResultScanner scanner = table.getScanner(scan);
        for(Result result: scanner) {
            String string = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
            System.out.println(string);
        }
        table.close();
    }
    @Test
    public void testDelete() throws IOException {
        HTable table = new HTable(configuration, TableName.valueOf("peoples"));
        Delete delete = new Delete(Bytes.toBytes("rk99999"));
        table.delete(delete);
        table.close();
    }
}
