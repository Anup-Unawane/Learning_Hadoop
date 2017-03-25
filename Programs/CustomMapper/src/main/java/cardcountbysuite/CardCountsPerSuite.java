package cardcountbysuite;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Submitting jar to hadoop cluster
 * hadoop jar /home/anup/Documents/mapreduce/mapreducedemo.jar cardcountbysuite.CardCountsPerSuite /anup/data/deckofcards.txt /anup/mapreduce/output/cardsPerSuite

 */

public class CardCountsPerSuite extends Configured implements Tool {

	public static class CardCountPerSuiteMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException 
		{
			context.write(new Text((value.toString().split("\\|"))[1]), new LongWritable(1));
		}
	}
	
	public int run(String[] args) throws Exception 
	{
		Job jb = Job.getInstance(getConf());
		
		//This line is required to run this program in cluster.
		//alternative to this is 
//		jb.setJar("Jar Name"); 
//		AS mapper class can be present in another jar. But this is not good practice
		jb.setJarByClass(getClass());
		
		
		
		//set mapper parameters
		jb.setMapperClass(CardCountPerSuiteMapper.class);
		jb.setMapOutputKeyClass(Text.class);
		jb.setMapOutputValueClass(LongWritable.class);
		
		jb.setPartitionerClass(HashPartitioner.class);
		
		//set Reducer parameters 
		jb.setReducerClass(LongSumReducer.class);
		
		jb.setNumReduceTasks(2);
		
		//set output key and value formats
		jb.setOutputKeyClass(Text.class);
		jb.setOutputValueClass(LongWritable.class);

		//set input and output file formats
		jb.setInputFormatClass(TextInputFormat.class);
		jb.setOutputFormatClass(TextOutputFormat.class);
		
		//set input & output paths
		FileInputFormat.setInputPaths(jb, new Path(args[0]));
		FileOutputFormat.setOutputPath(jb, new Path(args[1]));
		
		return jb.waitForCompletion(true) ? 0 :1;
		
		
	}

	public static void main(String[] args) throws Exception 
	{
		System.exit(ToolRunner.run(new CardCountsPerSuite(), args));
	}

}
