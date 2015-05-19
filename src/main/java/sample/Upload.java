package sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * Created by Cookie on 15/5/13.
 */

public class Upload {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        Path outPath = new Path(args[1]);
        int bufferSize = Integer.valueOf(args[2]);
        short replication = 1;
        long blockSize = 134217728L;

        InputStream in;

        if (inputPath.equals("-")) {
            in = new BufferedInputStream(System.in, bufferSize);
        } else {
            in = new BufferedInputStream(new FileInputStream(inputPath), bufferSize);
        }

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        OutputStream out = fs.create(outPath, true, bufferSize);
        IOUtils.copyBytes(in , out, bufferSize, true);
    }
}
