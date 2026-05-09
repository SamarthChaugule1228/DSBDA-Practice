/*
Design a distributed application using MapReduce which processes a log file of a system. 
List out the users who have logged for maximum period on the system. Use simple log file 
from the Internet and process it using a pseudo distribution mode on Hadoop platform.
*/

package Samarth;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Log {

    // Mapper
    public static class Map
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            try {

                String f[] = value.toString().split(",");

                String ip = f[1];

                SimpleDateFormat s =
                        new SimpleDateFormat("M/d/yyyy H:mm");

                Date in = s.parse(f[5]);
                Date out = s.parse(f[7]);

                int min = (int)
                        ((out.getTime() - in.getTime())
                                / (1000 * 60));

                context.write(new Text(ip),
                        new IntWritable(min));

            } catch (Exception e) {
            }
        }
    }

    // Reducer
    public static class Reduce
            extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        int max = 0;
        String user = "";

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context) {

            int total = 0;

            for (IntWritable i : values)
                total += i.get();

            if (total > max) {
                max = total;
                user = key.toString();
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(new Text(user),
                    new IntWritable(max));
        }
    }

    // Main Method
    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job =
                Job.getInstance(conf, "Log");

        job.setJarByClass(Log.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}
