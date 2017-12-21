package demo3;

import demo2.DataBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.swing.*;
import java.io.IOException;
import java.sql.CallableStatement;
import java.util.HashMap;
import java.util.Map;

public class DataCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DataCount.class);
        job.setMapperClass(DataCount.DataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //启动自定义分区
        job.setPartitionerClass(ProviderPartioner.class);

        job.setReducerClass(DataCount.DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.waitForCompletion(true);
    }

    public static class ProviderPartioner extends Partitioner<Text, DataBean> {

        public static Map<String ,Integer> providerMap = new HashMap<String ,Integer>();
        static {
            providerMap.put("135", 1);
            providerMap.put("136", 1);
            providerMap.put("137", 1);
            providerMap.put("138", 1);
            providerMap.put("139", 1);
            providerMap.put("150", 2);
            providerMap.put("159", 2);
            providerMap.put("182", 3);
            providerMap.put("183", 3);
        }
        public int getPartition(Text text, DataBean dataBean, int i) {
            String telNo = text.toString();
            String subTel = telNo.substring(0, 3);
            Integer num = providerMap.get(subTel);
            if(num == null) {
                return 0;
            }
            return num;
        }
    }


    public static class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        private  final static Log logger = LogFactory.getLog(demo2.DataCount.DataMapper.class);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
//            logger.info(line);
//            logger.info(fields.length);
            String telNo = fields[1];
            long up = Long.parseLong(fields[8]);
            long down = Long.parseLong(fields[9]);
            DataBean dataBean = new DataBean(telNo, up, down);
            Text text = new Text(telNo);
            context.write(text, dataBean);
        }
    }

    public static class DataReducer extends Reducer<Text, DataBean, Text, DataBean> {

        @Override
        protected void reduce(Text key, Iterable<DataBean> value, Context context) throws IOException, InterruptedException {
            long upSum = 0;
            long downSum = 0;
            for(DataBean bean: value) {
                upSum += bean.getUpPayLoad();
                downSum += bean.getDownPayLoad();
            }
            DataBean bean = new DataBean(key.toString(), upSum, downSum);
            context.write(key, bean);
        }

    }
}
