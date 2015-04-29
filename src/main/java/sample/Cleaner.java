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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cookie on 15/4/6.
 */
public class Cleaner extends Configured implements Tool {

    public static class DoMap extends Mapper<LongWritable, Text, Text, IntWritable> {



        DoMap () {

        }

        @Override
        public void map(LongWritable seq, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            Action action = Action.findKey(line);

            switch (action) {
                case Search:
                    break;
                case View:
                    break;
                case Cast:
                    break;
                case Order:
                    break;

            }

        }

        enum Action {
            Search("search"), View("view"), Cast("cast"), Order("order");

            private final String prefix = "act";
            private final String value;

            Action (String value) {
                this.value = value;
            }

            private String getKey(){
                return prefix + "=" +this.value;
            }

            public static Action findKey (String line) {
                Action result = null;
                for (Action action:values()) {
                    if(line.indexOf(action.getKey()) != -1){
                        result = action;
                        break;
                    }
                }
                return result;
            }
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

        job.setJarByClass(Cleaner.class);
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
            ToolRunner.run(new Cleaner(), args);
        }catch(Exception e){
            System.out.println("Usage: " + Cleaner.class.getSimpleName() + " [input] [output]");
        }
    }
}
