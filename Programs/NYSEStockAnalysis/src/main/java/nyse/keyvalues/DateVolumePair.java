package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 *	This class stores trade date and trade volume for that date 
 *
 */
public class DateVolumePair implements WritableComparable<DateVolumePair>
{
	private long tradeDate;
	private long tradeVolume;
	
	public DateVolumePair(Long tradeDate, Long tradevolume) {
		this.tradeDate = (tradeDate);
		this.tradeVolume = (tradevolume);
	}
	
	public DateVolumePair() {
	}
	
	public long getTradeDate() {
		return tradeDate;
	}

	public void setTradeDate(long tradeDate) {
		this.tradeDate = tradeDate;
	}
	
	public long getTradeVolume() {
		return tradeVolume;
	}
	
	public void setTradeVolume(long tradeVolume) {
		this.tradeVolume = tradeVolume;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(tradeDate);
		out.writeLong(tradeVolume);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		tradeDate = in.readLong();
		tradeVolume = in.readLong();
	}
	
	@Override
	public int compareTo(DateVolumePair dv) {
		int cmp = compare(tradeDate, dv.tradeDate);
		if(cmp == 0)
			return compare(tradeVolume, dv.tradeVolume);
		return cmp;
	}

	public static int compare(long a, long b)
	{
		return (a < b ? -1 : (a == b ? 0 : 1));
	}
	
	@Override
	public String toString() {
		return tradeDate + "\t" + tradeVolume ;
	}

	
}
