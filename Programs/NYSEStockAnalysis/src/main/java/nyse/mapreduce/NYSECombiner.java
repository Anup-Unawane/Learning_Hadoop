package nyse.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import nyse.keyvalues.StockMonthPair;
import nyse.keyvalues.VolumeCountPair;

/**
 * This class combines all VolumeCountPairs for a StockMonthPair and 
 * produces a single VolumeCountPair with sum of Volume and Count resp.of
 * all other VolumeCountPairs
 * 
 * This is Combiner class.
 */

public class NYSECombiner extends Reducer<StockMonthPair, VolumeCountPair, 
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
		
		VolumeCountPair vcpair = new VolumeCountPair(totalVolume, totalCount);
		context.write(smpair, vcpair);
	}
}
