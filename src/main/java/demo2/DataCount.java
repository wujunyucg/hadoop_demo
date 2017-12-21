package demo2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DataCount.class);
        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

    public static class DataMapper extends Mapper<LongWritable, Text, Text, DataBean> {
        private  final static Log logger = LogFactory.getLog(DataMapper.class);
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
