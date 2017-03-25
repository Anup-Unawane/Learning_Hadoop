package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class stored trade month and stock ticker name.
 * trade month is in format yyyy-MM 
 *
 */
public class StockMonthPair implements WritableComparable<StockMonthPair> 
{
	private Text tradeMonth;
	private Text stockTicker;

	public Text getStockTicker() {
		return stockTicker;
	}

	public void setStockTicker(Text stockTicker) {
		this.stockTicker = stockTicker;
	}

	public Text getTradeMonth() {
		return tradeMonth;
	}

	public void setTradeMonth(Text tradeMonth) {
		this.tradeMonth = tradeMonth;
	}

	public StockMonthPair() 
	{
		super();
		this.tradeMonth = new Text();
		this.stockTicker = new Text();
	}

	public StockMonthPair(Text tradeMonth, Text stockTicker) 
	{
		super();
		this.tradeMonth = tradeMonth;
		this.stockTicker = stockTicker;
	}
	
	public StockMonthPair(String tradeMonth, String stockTicker) 
	{
		super();
		this.tradeMonth = new Text(tradeMonth);
		this.stockTicker = new Text(stockTicker);
	}
	
	public void write(DataOutput out) throws IOException {
		this.tradeMonth.write(out);
		this.stockTicker.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.tradeMonth.readFields(in);
		this.stockTicker.readFields(in);
	}

	public int compareTo(StockMonthPair o) {
		int cmp = tradeMonth.compareTo(o.tradeMonth);
		if (cmp == 0)
			return stockTicker.compareTo(o.stockTicker);
		return cmp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tradeMonth == null) ? 0 : tradeMonth.hashCode());
		result = prime * result + ((stockTicker == null) ? 0 : stockTicker.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StockMonthPair other = (StockMonthPair) obj;
		if (tradeMonth == null) {
			if (other.tradeMonth != null)
				return false;
		} else if (!tradeMonth.equals(other.tradeMonth))
			return false;
		if (stockTicker == null) {
			if (other.stockTicker != null)
				return false;
		} else if (!stockTicker.equals(other.stockTicker))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return tradeMonth + "\t" + stockTicker;
	}

}
