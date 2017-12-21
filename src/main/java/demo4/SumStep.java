package demo4;

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

public class SumStep {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SumStep.class);
        job.setMapperClass(SumStep.SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


    public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private  final static Log logger = LogFactory.getLog(demo2.DataCount.DataMapper.class);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double outcome = Double.parseDouble(fields[2]);
            InfoBean infoBean = new InfoBean(account, income, outcome);
            Text text = new Text(account);
            context.write(text, infoBean);
        }
    }

    public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {

        @Override
        protected void reduce(Text key, Iterable<InfoBean> value, Context context) throws IOException, InterruptedException {
            double income = 0;
            double outcome = 0;
            for (InfoBean bean: value) {
                income += bean.getIncome();
                outcome += bean.getOutcome();
            }
            InfoBean bean = new InfoBean(key.toString(), income, outcome);
            context.write(key, bean);
        }

    }
}
