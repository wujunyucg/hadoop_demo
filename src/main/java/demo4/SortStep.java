package demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortStep {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SortStep.class);
        job.setMapperClass(SortStep.SortMapper.class);
        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(SortStep.SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


    public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double outcome = Double.parseDouble(fields[2]);
            InfoBean infoBean = new InfoBean(account, income, outcome);
            context.write(infoBean, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean> {

        @Override
        protected void reduce(InfoBean key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
            Text text = new Text(key.getAccount());
            context.write(text, key);
        }

    }
}
