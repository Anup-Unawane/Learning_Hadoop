package nyse.mapreduce;

import java.io.IOException;

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
public class NYSEMapper extends
		Mapper<LongWritable, Text, StockMonthPair, VolumeCountPair> 
{
	private static String TARGET_DATE_FORMAT = "yyyy-MM";
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, StockMonthPair, VolumeCountPair>.Context context)
			throws IOException, InterruptedException 
	{
		NYSEStock stock = NYSEStockParser.parse(value.toString(), TARGET_DATE_FORMAT);
		StockMonthPair mapOPKey = new StockMonthPair(stock.getStockTicker(), stock.getTradeDate());
		VolumeCountPair mapOPVal = new VolumeCountPair(new LongWritable(stock.getVolume()), new LongWritable(1));
		
		context.write(mapOPKey, mapOPVal);
	}
}
