package all_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Cleaner extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        final String inputPath = args[0];
        final String outPath = args[1];

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf);
        job.setJarByClass(Cleaner.class);
        FileInputFormat.setInputPaths(job, inputPath);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
        return 0;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Cleaner(), args);
    }
    static class MyMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
        //实例化一个转换类
        LogParser parser=new LogParser();
        //之所以把v2放到map方法的外部是为了一次实例化多次调用，避免资源的浪费
        Text v2=new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            //获取到日志文件的一行内容(value就是v1的值)
            final String line=value.toString();
            //将这一行内容经过转换类转化为一个数组
            final String[] parsed=parser.parse(line);
            //数组中第一个元素的值便是IP
            final String ip=parsed[0];
            //数组中第二个元素的值是log产生的时间
            final String logtime=parsed[1];
            //数组中第三个元素的值是url
            String url=parsed[2];
            //我们要过滤掉以"GET /static"或"GET /uc_server"开头的数据（我们姑且认为这两个开头的数据是坏数据）
            if(url.startsWith("GET /static") || url.startsWith("GET /uc_server")){
                return;
            }
            //如果是GET请求，我们截取"GET"和" HTTP/1.1"之间的数据，比如"GET /static/image/common/faq.gif HTTP/1.1"
            //我们要得到的是"/static/image/common/faq.gif"
            if(url.startsWith("GET")){
                url=url.substring("GET ".length()+1, url.length()-" HTTP/1.1".length());
            }
            //如果是POST请求，我们截取"POST"和" HTTP/1.1"之间的数据，比如"POST /api/manyou/my.php HTTP/1.0"
            //我们要得到的是"/api/manyou/my.php HTTP/1.0"
            if(url.startsWith("POST")){
                url=url.substring("POST ".length()+1,url.length()-" HTTP/1.1".length());
            }
            //v2的输出形式是：ip  logtime url
            v2.set(ip+"\t"+logtime+"\t"+url);
            //k2和k1一样，都是数值型
            context.write(key, v2);
        }
    }

    static class MyReducer extends Reducer<LongWritable, Text,Text,NullWritable> {
        @Override
        protected void reduce(LongWritable k2, Iterable<Text> v2s,
                              Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            //Reducer要做的工作其实非常简单，就是把<k2,v2>的v2的值给输出出去，k3的类型是Text，v3其实是多余的，那么我们用Null来表示。序列化形式的
            //Null是NullWritable.get()
            for(Text v2:v2s){
                context.write(v2, NullWritable.get());
            }
        }
    }

}


class LogParser {
    public static final SimpleDateFormat FORMAT =
            new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    public static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] args) {
        final String s1="27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
        LogParser parser=new LogParser();
        final String[] array=parser.parse(s1);
        System.out.println("样例数据："+s1);
        System.out.format("解析结果：ip=%s, time=%s, url=%s, status=%s, traffic=%s",array[0],array[1],array[2],array[3],array[4]);
    }
    /**
     * 解析日志的行记录
     * @param line
     * @return 数组含有5个元素，分别是ip、时间、url、状态、流量
     */
    public String[] parse(String line){
        String ip=parseIP(line);
        String time;
        try{
            time=parseTime(line);
        }catch(Exception e){
            time="null";
        }
        String url;
        try{
            url=parseURL(line);
        }catch(Exception e){
            url="null";
        }
        String status=parseStatus(line);
        String traffic=parseTraffic(line);
        return new String[]{ip,time,url,status,traffic};
    }

    /**
     * 获取本次浏览所消耗的流量
     * 字符串中关于流量的信息如："GET /static/image/common/faq.gif HTTP/1.1" 200 1127
     * 我们要得到的是1127，为了得到它，我们从最后一个"\"后的空格开始，截取到最后，然后去掉两端的空格，就剩"200 1127"
     * 然后我们把"200 1127"以空格为分隔符，数组的第二个元素的值就是"1127"
     * @param line
     * @return
     */
    private String parseTraffic(String line){
        final String trim=line.substring(line.lastIndexOf("\"")+1).trim();
        String traffic=trim.split(" ")[1];
        return traffic;
    }

    /**
     * 截取访问结果Status
     * 字符串中关于Status的信息如："GET /static/image/common/faq.gif HTTP/1.1" 200 1127
     * 我们要得到的是200，为了得到它，我们从最后一个"\"后的空格开始，截取到最后，然后去掉两端的空格，就剩"200 1127"
     * 然后我们把"200 1127"以空格为分隔符，数组的第一个元素的值就是"200"
     * @param line
     * @return
     */
    private String parseStatus(String line){
        String trim;
        try{
            trim=line.substring(line.lastIndexOf("\"")+1).trim();
        }catch(Exception e){
            trim="null";
        }
        String status=trim.split(" ")[0];
        return status;
    }

    /**
     * 截取字符串中的URL
     * 字符串中关于URL的信息如："GET /static/image/common/faq.gif HTTP/1.1"
     * 我们截取的话当然应该从"\"的下一个字母开始，到下一个"\"结束（字符串截取包括前面，不包括后面）
     * @param line
     * @return
     */
    private String parseURL(String line){
        final int first=line.indexOf("\"");
        final int last=line.lastIndexOf("\"");
        String url=line.substring(first+1,last);
        return url;
    }

    /**
     * 将英文时间转变为如：20130530135026这样形式的时间
     * 字符串中关于时间的信息如： [30/May/2013:17:38:20 +0800] ，我们截取其中的时间，截取的开始位置是"["后面的"3"，
     * 结束的位置是"+0800"，然后去掉前后的空格就是我们想要的英文时间"30/May/2013:17:38:20"
     * 有了英文时间，我们便使用FORMAT.parse方法将time转换为时间，然后使用DATEFORMAT.format方法将时间转换为我们想要的"20130530173820"
     * @param line
     * @return
     */
    private String parseTime(String line){
        final int first=line.indexOf("[");
        final int last=line.indexOf("+0800]");
        String time=line.substring(first+1,last).trim();
        try{
            return DATE_FORMAT.format(FORMAT.parse(time));
        }catch(Exception e){
            e.printStackTrace();
        }
        return "";
    }
    /**
     * 截取字符串中的IP 字符串如：27.19.74.143 - - [30/May/2013:17:38:20 +0800]
     * 我们以"- -"为分割符，数组的第一个值便是IP的值
     * @param line
     * @return
     */
    private String parseIP(String line){
        String ip=line.split("- -")[0].trim();
        return ip;
    }
}
