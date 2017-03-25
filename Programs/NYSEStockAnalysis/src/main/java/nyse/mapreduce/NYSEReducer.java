package nyse.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import nyse.keyvalues.StockMonthPair;
import nyse.keyvalues.VolumeCountPair;

/**
 * This class combines all VolumeCountPairs for a StockMonthPair and 
 * produces a single VolumeCountPair with Average of Volume resp.of
 * all other VolumeCountPairs
 * 
 * This is Reducer class.
 */

public class NYSEReducer extends Reducer<StockMonthPair, VolumeCountPair, 
										  StockMonthPair, VolumeCountPair> 
{
	@Override
	public void reduce(StockMonthPair smpair, Iterable<VolumeCountPair> vcpairs, 
						  Reducer<StockMonthPair,VolumeCountPair,StockMonthPair,VolumeCountPair>.Context context)
							throws IOException ,InterruptedException 
	{
		Long totalVolume = 0L;
		Long totalCount = 0L;
		
		for(VolumeCountPair vcp : vcpairs)
		{
			totalVolume += vcp.getStockVolume().get();
			totalCount += vcp.getCount().get();
		}
		
		Long avgVolume = totalVolume / totalCount;
		
		VolumeCountPair vcpair = new VolumeCountPair(avgVolume, totalCount);
		context.write(smpair, vcpair);
	}
}
