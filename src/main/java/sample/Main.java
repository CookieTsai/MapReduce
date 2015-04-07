package sample;

import org.apache.hadoop.fs.Path;

/**
 * Created by Cookie on 15/4/6.
 */
public class Main {
    public static void main(String[] args) {
        try{
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            new WordCount(inputPath, outputPath).execute();
        }catch(Exception e){
            System.out.println("Usage: "+Main.class.getSimpleName()+" [input] [output]");
        }
    }
}
