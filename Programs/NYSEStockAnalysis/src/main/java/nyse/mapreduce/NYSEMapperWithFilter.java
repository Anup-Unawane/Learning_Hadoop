package nyse.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nyse.keyvalues.VolumeCountPair;
import nyse.keyvalues.StockMonthPair;
import nyse.parser.NYSEStockParser;
import nyse.stock.NYSEStock;

/**
 * This class maps each stock volume with its name and trade date. 
 *
 */
public class NYSEMapperWithFilter extends
Mapper<LongWritable, Text, StockMonthPair, VolumeCountPair> 
{
	private static String TARGET_DATE_FORMAT = "yyyy-MM";
	private Set<String> stocksToFilter = new HashSet<>();

	/**
	 * This method is called once per mapper execution. If some initialization is 
	 * required for map-reduce, can be done in this method
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, StockMonthPair, VolumeCountPair>.Context context)
			throws IOException, InterruptedException 
	{
		super.setup(context);
		String filterStocks = context.getConfiguration().get("filter.by.stockticker");
		if(filterStocks != null)
			stocksToFilter.addAll(Arrays.asList(filterStocks.split(",")));
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, StockMonthPair, VolumeCountPair>.Context context)
					throws IOException, InterruptedException 
	{
		NYSEStock stock = NYSEStockParser.parse(value.toString(), TARGET_DATE_FORMAT);

		if(stocksToFilter.isEmpty() || stocksToFilter.contains(stock.getStockTicker()))
		{
			StockMonthPair mapOPKey = new StockMonthPair(stock.getStockTicker(), stock.getTradeDate());
			VolumeCountPair mapOPVal = new VolumeCountPair(new LongWritable(stock.getVolume()), new LongWritable(1));

			context.write(mapOPKey, mapOPVal);
		}
	}
}
