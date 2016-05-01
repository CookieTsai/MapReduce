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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cookie on 15/4/6.
 */
public class OrderCountV1 extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static Pattern pattern = Pattern.compile("([0-9]+),([0-9]+),([0-9]+)");
        private final static String ONE = "1";
        private final static String TWO = "2";

        private Text word = new Text();
        private IntWritable price = new IntWritable();
        private String count;
        private int money;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength() != 0 ){

                Matcher matcher = pattern.matcher(value.toString());

                while (matcher.find()) {
                    word.set(matcher.group(1));

                    count = matcher.group(2);
                    money = Integer.parseInt(matcher.group(3));

                    if (count.equals(ONE)) {
                        price.set(money);
                    } else if (count.equals(TWO)) {
                        price.set(2 * money);
                    } else {
                        price.set(Integer.parseInt(count) * money);
                    }

                    context.write(word, price);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable sumPrice = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value:values) {
                sum += value.get();
            }
            sumPrice.set(sum);
            context.write(key, sumPrice);
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

        job.setJarByClass(OrderCountV1.class);
        job.setJobName("OrderCountV1");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // input
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(Reduce.class);

        // reducer
        job.setReducerClass(Reduce.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);


        return job.waitForCompletion(true)? 0 : -1 ;
    }

    public static void main(String[] args) {
        try{
            ToolRunner.run(new OrderCountV1(), args);
        }catch(Exception e){
            System.out.println("Usage: " + OrderCountV1.class.getName() + " [input] [output]");
        }
    }
}
