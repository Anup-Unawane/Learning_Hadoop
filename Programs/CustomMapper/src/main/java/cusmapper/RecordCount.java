package cusmapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *	This Map-Reduce program outputs number of records in a file. 
 *
 */
@SuppressWarnings("unused")
public class RecordCount extends Configured implements Tool 
{
	private static class RecordMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException 
		{
			context.write(new Text("Count"), new LongWritable(1));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setMapperClass(RecordMapper.class);
		//output of mapper stage is in format as <Count, 1>
		job.setMapOutputKeyClass(Text.class); //Count
		job.setMapOutputValueClass(LongWritable.class);	 //1	

		job.setReducerClass(LongSumReducer.class);

		//To check Map Stage intermediate output, comment setReducerClass line and set
		//setNumReduceTasks to 0
		//job.setNumReduceTasks(0);

		//output of reducer stage is in format as <Count, 52>
		job.setOutputKeyClass(Text.class);	//Count
		job.setOutputValueClass(LongWritable.class);	//sum of counts

		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception 
	{
		System.exit(ToolRunner.run(new RecordCount(), args));
	}

}
