package nyse.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import nyse.keyvalues.StockMonthPair;
import nyse.keyvalues.VolumeCountPair;

public class StockPartitioner extends Partitioner<StockMonthPair, VolumeCountPair>
{
	// here numPartitions is number of reduce tasks set using API: Job.setNumReduceTasks
	
	@Override
	public int getPartition(StockMonthPair key, VolumeCountPair value, int numPartitions) {
		
		return Math.abs((key.getStockTicker().hashCode() & Integer.MAX_VALUE)) % numPartitions;
	}

}
