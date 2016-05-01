package sample.wordcount;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Cookie on 15/4/6.
 */
public class Main {

    private Path inputPath;
    private Path outputPath;

    public Main(Path inputPath, Path outputPath){
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ONE_COUNT = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, ONE_COUNT);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

    public void execute() throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf);
        job.setJobName("wordCount");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // input
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //job.setCombinerClass(Reduce.class);

        // reducer
        job.setReducerClass(Reduce.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(0);

        job.setJarByClass(job.getMapperClass());
        job.waitForCompletion(true);

    }

    public static void main(String[] args) {
        try{
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            new Main(inputPath, outputPath).execute();
        }catch(Exception e){
            System.out.println("Usage: " + Main.class.getSimpleName() + " [input] [output]");
        }
    }
}
