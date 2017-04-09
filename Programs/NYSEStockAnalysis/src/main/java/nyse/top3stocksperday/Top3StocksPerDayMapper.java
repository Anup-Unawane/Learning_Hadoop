package nyse.top3stocksperday;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nyse.keyvalues.DateVolumePair;
import nyse.parser.NYSEStockParser;
import nyse.stock.NYSEStock;

//mapper input is record count, record string
//mapper output is DateVolumePair, record String

public class Top3StocksPerDayMapper extends 
				Mapper<LongWritable, Text, DateVolumePair, Text> 
{
	private static String TARGET_DATE_FORMAT = "yyyyMMdd";

	@Override
	protected void map(LongWritable key, Text record, Mapper<LongWritable, Text, DateVolumePair, Text>.Context context)
			throws IOException, InterruptedException {
		NYSEStock stock = NYSEStockParser.parse(record.toString(), TARGET_DATE_FORMAT);
		DateVolumePair pair = new DateVolumePair(Long.parseLong(stock.getTradeDate()), stock.getVolume());
		context.write(pair, record);
		
	}
}
