package demo5;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableCURD {
    private final static Log logger = LogFactory.getLog(HBaseTableCURD.class);
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (args[0].equals("put")) {
            HTable table = new HTable(configuration, TableName.valueOf("peoples"));
            Put put = new Put(Bytes.toBytes("rk0001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhanshan"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(800000));
            table.put(put);
            table.close();
        } else if (args[0].equals("putAll")) {
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
        } else if (args.equals("get")) {
            HTable table = new HTable(configuration, TableName.valueOf("peoples"));
            Get get = new Get(Bytes.toBytes("rk99999"));
            Result result = table.get(get);
            String string = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
            logger.info(string);
            table.close();
        } else if (args.equals("scan")) {
            HTable table = new HTable(configuration, TableName.valueOf("peoples"));
            Scan scan = new Scan(Bytes.toBytes("rk29990"), Bytes.toBytes("rk30000"));
            ResultScanner scanner = table.getScanner(scan);
            for(Result result: scanner) {
                String string = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
                logger.info(string);
            }
            table.close();
        } else if(args.equals("delete")) {
            HTable table = new HTable(configuration, TableName.valueOf("peoples"));
            Delete delete = new Delete(Bytes.toBytes("rk99999"));
            table.delete(delete);
            table.close();
        }
    }

}
