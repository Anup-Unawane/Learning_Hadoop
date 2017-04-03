package nyse.mapreduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.keyvalues.VolumeCountPair;
import nyse.partitioners.MonthPartitioner;
import nyse.partitioners.StockPartitioner;
import nyse.keyvalues.StockMonthPair;

/**
 * Map-Reduce Program to calculate Average volume of stock traded per month for year 2012 and 2013
 * Run this class with 4 arguments
 * 1. With parameter to pass to MR application as
 * 	-Dfilter.by.stockticker=<stocks to filter>
 * 2. Input directory
 * 3. Regex Expression to filter out required files
 * 4. Output Directory
 * e.g.
 * -Dfilter.by.stockticker=BAC,APL /home/anup/Datasets/nyse/ nyse_201[2-3]* /home/anup/work/mapreduce/avgstockpermonthfilter
 * 
 * as data is stored in .csv files, so added wild card * to get all files satisfying regex.
 * If data is stored in seperate directories having names as nyse_2012, nyse_2013,
 * no need to give wild card character *
 * 
 * for execution on cluster run:
 * hadoop jar /home/anup/Documents/mapreduce/nyse_avg_stock_per_month_filtered.jar 
 * 				-Dfilter.by.stockticker=BAC,APL /anup/data/nyse/  
 * 				 nyse_201[2-3]* 
 * 				 /anup/mapreduce/output/avgstockpermonthfiltered
 * 
 */

public class NYSEYearFilterExecutor extends Configured implements Tool 
{
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		
		//We are accessing file system to read data from filtered files
		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
		
		Path path = new Path(args[0] + args[1]);
		
		FileStatus fstatus[] = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(fstatus);
		for(Path p : paths)
		{
			System.out.println(p.toString());
			FileInputFormat.addInputPath(job, p);
		}
 		
		/* If input directory contains large number of smaller size text files, 
		 * more number of mapper instances are created(1 per file) which is 
		 * very inefficient. So, we need to use CombineTextInputFormat class 
		 */
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(NYSEMapperWithFilter.class);
		
		job.setMapOutputKeyClass(StockMonthPair.class);
		job.setMapOutputValueClass(VolumeCountPair.class);
		
		job.setCombinerClass(NYSECombiner.class);
		job.setReducerClass(NYSEReducer.class);
		
		job.setOutputKeyClass(StockMonthPair.class);
		job.setOutputValueClass(VolumeCountPair.class);
		
		job.setPartitionerClass(StockPartitioner.class);
		
		job.setNumReduceTasks(4);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		System.exit(ToolRunner.run(new NYSEYearFilterExecutor(), args));
	}
}
