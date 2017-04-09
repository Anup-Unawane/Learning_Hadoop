package nyse.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import nyse.keyvalues.DateVolumePair;

public class DatePartitioner extends Partitioner<DateVolumePair, Text> {

	@Override
	public int getPartition(DateVolumePair key, Text value, int numPartitions) {
		Long date = key.getTradeDate();
		
		int month = new Integer(date.toString().substring(0,6));
		
		return (month % numPartitions);
	}

}
