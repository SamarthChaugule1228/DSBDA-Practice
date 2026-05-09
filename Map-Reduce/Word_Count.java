package Samarth;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Log {
	// Mapper Class
    public static class Map
            extends Mapper<Object, Text,
            Text, IntWritable> {

        public void map(Object k,
                        Text v,
                        Context c)
                throws IOException, InterruptedException {
        	   // Split line into words
            String words[] =
                    v.toString().split(" ");

            // Send each word with count 1
            for(String w : words) {

                c.write(new Text(w),
                        new IntWritable(1));
            }
        }
    }
    public static class Reduce
    extends Reducer<Text, IntWritable,
    Text, IntWritable> {

public void reduce(Text k,
                   Iterable<IntWritable> v,
                   Context c)
        throws IOException, InterruptedException {

    int sum = 0;
    // Add all counts
    for(IntWritable i : v) {

        sum += i.get();
    }

    // Final Output
    c.write(k,new IntWritable(sum));
}
        }

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		  Configuration conf =
	                new Configuration();

	        Job job =
	                Job.getInstance(conf, "wordcount");

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
	                job.waitForCompletion(true)
	                        ? 0 : 1
	        );

	}

}
