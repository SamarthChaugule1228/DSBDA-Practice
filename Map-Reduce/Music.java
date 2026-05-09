package Samarth;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Log {

	public static class Map extends Mapper<Object, Text, Text, IntWritable>
	{
			public void map (Object k, Text v, Context c)throws IOException, InterruptedException
			{
				try {

	                // Split line using comma
	                String f[] =v.toString().split(",");

	                // Skip header
	                if(f[0].equals("UserId"))
	                    return;

	                // TrackId
	                String track =
	                        f[1];

	                // Shared value
	                int shared =
	                        Integer.parseInt(f[2]);
	                // Send data
	                c.write(
	                        new Text(track),
	                        new IntWritable(shared)
	                );

	            } catch(Exception e) {

	            }
				
			}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce (Text k, Iterable <IntWritable> v, Context c)throws IOException, InterruptedException
		{
			int totalShare = 0;

            int listeners = 0;

            for(IntWritable i : v) {

                totalShare += i.get();

                listeners++;
            }
            c.write(
                    new Text(k.toString()
                            + " UniqueListeners="
                            + listeners
                    ),
                    new IntWritable(totalShare)
            );
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"musiccount");
		job.setJarByClass(Log.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
