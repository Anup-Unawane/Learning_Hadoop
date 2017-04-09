package nyse.top3stocksperday;

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

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import nyse.comparators.DateVolumePairComparator;
import nyse.comparators.DateVolumePairGroupingComparator;
import nyse.keyvalues.DateVolumePair;
import nyse.partitioners.DatePartitioner;

/**
 * Top 3 stocks per day for year 2012-2013 
 *
 */

public class Top3StocksPerDayExecutor extends Configured implements Tool 
{
	@Override
	public int run(String[] args) throws Exception {
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
		
		//Input Details
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		//Mapper details
		job.setMapperClass(Top3StocksPerDayMapper.class);
		job.setMapOutputKeyClass(DateVolumePair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		job.setPartitionerClass(DatePartitioner.class);
		job.setGroupingComparatorClass(DateVolumePairGroupingComparator.class);
		job.setSortComparatorClass(DateVolumePairComparator.class);
		
		//Reducer Details
		job.setReducerClass(Top3StocksPerDayReducer.class);
		job.setOutputKeyClass(DateVolumePair.class);
		job.setOutputValueClass(Text.class);
		
		//Output Details
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)?0 : 1;
	}

	public static void main(String[] args) throws Exception {
 		System.out.println( (ToolRunner.run(new Top3StocksPerDayExecutor(), args)));
 		System.out.println("Done!!!");

	}
}
