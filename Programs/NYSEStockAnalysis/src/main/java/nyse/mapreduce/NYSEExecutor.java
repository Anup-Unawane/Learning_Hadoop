package nyse.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.keyvalues.VolumeCountPair;
import nyse.keyvalues.StockMonthPair;

/**
 * Map-Reduce Program to calculate Average volume of stock traded per month
 *  Run this class with 3 arguments
 * 1. Input directory
 * 2. Output Directory
 * e.g.
 * /home/anup/Datasets/nyse/ /home/anup/work/mapreduce/avgstockpermonth
 * 
 */

public class NYSEExecutor extends Configured implements Tool 
{
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		/* If input directory contains large number of smaller size text files, 
		 * more number of mapper instances are created(1 per file) which is 
		 * very inefficient. So, we need to use CombineTextInputFormat class 
		 */
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(NYSEMapper.class);
		
		job.setMapOutputKeyClass(StockMonthPair.class);
		job.setMapOutputValueClass(VolumeCountPair.class);
		
		job.setCombinerClass(NYSECombiner.class);
		job.setReducerClass(NYSEReducer.class);
		
		job.setOutputKeyClass(StockMonthPair.class);
		job.setOutputValueClass(VolumeCountPair.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		System.exit(ToolRunner.run(new NYSEExecutor(), args));
	}
}
