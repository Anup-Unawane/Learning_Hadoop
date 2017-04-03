package nyse.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import nyse.keyvalues.StockMonthPair;
import nyse.keyvalues.VolumeCountPair;

public class MonthPartitioner extends Partitioner<StockMonthPair, VolumeCountPair> {

	@Override
	public int getPartition(StockMonthPair key, VolumeCountPair value, int numPartitions) {
		return Math.abs((key.getTradeMonth().hashCode() & Integer.MAX_VALUE)) % numPartitions;
	}

}
