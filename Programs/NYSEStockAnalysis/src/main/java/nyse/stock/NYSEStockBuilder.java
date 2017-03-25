package nyse.stock;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NYSEStockBuilder {
	private String stockTicker;
	private String tradeDate;
	private Float openPrice;
	private Float highPrice;
	private Float lowPrice;
	private Float closePrice;
	private Long volume;

	public NYSEStockBuilder setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
		return this;
	}
	public NYSEStockBuilder setTradeDate(String tradeDate) {
		this.tradeDate = tradeDate;
		SimpleDateFormat sf1 = new SimpleDateFormat("dd-MMM-yyyy");
		SimpleDateFormat sf2 = new SimpleDateFormat("yyyy-MM");
		
		Date dt1 = new Date();
		try {
			dt1 = sf1.parse(tradeDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		this.tradeDate = sf2.format(dt1);
		return this;
	}
	public NYSEStockBuilder setOpenPrice(Float openPrice) {
		this.openPrice = openPrice;
		return this;
	}
	public NYSEStockBuilder setHighPrice(Float highPrice) {
		this.highPrice = highPrice;
		return this;
	}
	public NYSEStockBuilder setLowPrice(Float lowPrice) {
		this.lowPrice = lowPrice;
		return this;
	}
	public NYSEStockBuilder setClosePrice(Float closePrice) {
		this.closePrice = closePrice;
		return this;
	}
	public NYSEStockBuilder setVolume(Long volume) {
		this.volume = volume;
		return this;
	}

	public NYSEStock buildNYSEStock()
	{
		NYSEStock stock = new NYSEStock();

		stock.setClosePrice(closePrice);
		stock.setHighPrice(highPrice);
		stock.setLowPrice(lowPrice);
		stock.setOpenPrice(openPrice);
		stock.setStockTicker(stockTicker);
		stock.setTradeDate(tradeDate);
		stock.setVolume(volume);

		return stock;
	}
}
