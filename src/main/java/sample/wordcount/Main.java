package sample.wordcount;

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
import java.util.StringTokenizer;

/**
 * Created by Cookie on 15/4/6.
 */
<<<<<<< HEAD:src/main/java/sample/WordCount.java
public class WordCount extends Configured implements Tool {

    public static class DoMap extends Mapper<LongWritable, Text, Text, IntWritable> {
=======
public class Main {

    private Path inputPath;
    private Path outputPath;

    public Main(Path inputPath, Path outputPath){
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
>>>>>>> master:src/main/java/sample/wordcount/Main.java

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

    public static class DoReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
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

        job.setNumReduceTasks(5);

        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // input
        job.setInputFormatClass(TextInputFormat.class);

        // mapper
        job.setMapperClass(DoMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer
        job.setReducerClass(DoReduce.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)? 0 : -1 ;
    }

    public static void main(String[] args) {
        try{
<<<<<<< HEAD:src/main/java/sample/WordCount.java
            ToolRunner.run(new WordCount(), args);
=======
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            new Main(inputPath, outputPath).execute();
>>>>>>> master:src/main/java/sample/wordcount/Main.java
        }catch(Exception e){
            System.out.println("Usage: " + Main.class.getName() + " [input] [output]");
        }
    }
}
