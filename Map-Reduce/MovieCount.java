package moviedemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieCount {
	public static class Map extends Mapper<Object, Text, Text, FloatWritable>
	{
		public void map(Object k, Text v, Context c)throws IOException,InterruptedException
		{
			try{
				String f[]=v.toString().split(",");
				
				if(f[0].equals("UserId"))
					return;
				
				String movie=f[1];
				float rating=Float.parseFloat(f[2]);
				
				c.write(new Text(movie), new FloatWritable(rating));
				
				
			}
			catch(Exception e)
			{}
			
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void reduce(Text k, Iterable<FloatWritable> v, Context c)throws IOException,InterruptedException
		{
			float total=0;
			int count=0;
			
			for(FloatWritable i:v)
			{
				total+=i.get();
				count++;
			}
			
			if(count!=0)
			{
				float avg=total/count;
				if(avg>=5.0)
				{
					c.write(k, new FloatWritable(avg));
				}
				
			}
			
		}
	}
	
	

	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"moviecount");
		job.setJarByClass(MovieCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1); 
	}

}