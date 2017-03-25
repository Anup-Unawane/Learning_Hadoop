package nyse.parser;

import nyse.stock.NYSEStock;
import nyse.stock.NYSEStockBuilder;

public class NYSEStockParser 
{
	public static NYSEStock parse(String record)
	{
		String []data = record.split(",");
		
		if(data.length == 0)
			return null;
		
		String stockTicker = data[0];
		String tradeDate = data[1];
		
		Float openPrice = Float.valueOf(data[2]);
		Float highPrice = Float.valueOf(data[3]);
		Float lowPrice = Float.valueOf(data[4]);
		Float closePrice = Float.valueOf(data[5]);
		
		Long volume = Long.valueOf(data[6]);
		
		
		NYSEStock stock = new NYSEStockBuilder()
								  .setStockTicker(stockTicker)
								  .setTradeDate(tradeDate)
								  .setOpenPrice(openPrice)
								  .setHighPrice(highPrice)
								  .setLowPrice(lowPrice)
								  .setClosePrice(closePrice)
								  .setVolume(volume)
								  .buildNYSEStock();
		
		return stock;
	}
}
