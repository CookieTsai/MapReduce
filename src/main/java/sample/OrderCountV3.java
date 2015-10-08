package sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cookie on 15/4/6.
 */
public class OrderCountV3 extends Configured implements Tool {

    public static class DoMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static Pattern pattern = Pattern.compile("([0-9]+),([0-9]+),([0-9]+)");
        private final static Map<String, IntWritable> map = new HashMap<String, IntWritable>();
        private final static String ONE = "1";

        private String key;
        private Integer orderPrice;
        private Text outKey = new Text();

        @Override
        public void map(LongWritable seq, Text value, Context context) throws IOException, InterruptedException {
            Matcher m = pattern.matcher(value.toString());
            while (m.find()) {
                key = m.group(1);
                orderPrice = getOrderPrice(key, getPrice(m.group(2), m.group(3)));
                map.put(key, new IntWritable(orderPrice));
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String key:map.keySet()) {
                outKey.set(key);
                context.write(outKey, map.get(key));
            }
        }
        private Integer getOrderPrice(String key, Integer price) {
            return (map.get(key) == null)? price:price + map.get(key).get();
        }
        private static Integer getPrice(String count, String productPrice) {
            return (count.equals(ONE))? Integer.valueOf(productPrice):Integer.valueOf(count) * Integer.valueOf(productPrice);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);

        job.setNumReduceTasks(0);

        job.setJarByClass(OrderCountV3.class);
        job.setJobName("OrderCountV3");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // input
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(DoMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)? 0 : -1 ;
    }

    public static void main(String[] args) {
        try{
            ToolRunner.run(new OrderCountV3(), args);
        }catch(Exception e){
            System.out.println("Usage: " + OrderCountV3.class.getSimpleName() + " [input] [output]");
        }
    }
}
