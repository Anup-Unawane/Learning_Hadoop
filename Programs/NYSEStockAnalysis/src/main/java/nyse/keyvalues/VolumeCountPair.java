package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class stores stock volume per day and count as count 
 * to measure number of times traded. 
 *
 */
public class VolumeCountPair implements WritableComparable<VolumeCountPair> 
{
	private LongWritable stockVolume;
	public LongWritable getStockVolume() {
		return stockVolume;
	}

	public void setStockVolume(LongWritable stockVolume) {
		this.stockVolume = stockVolume;
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	private LongWritable count;

	public VolumeCountPair() 
	{
		super();
		this.stockVolume = new LongWritable();
		this.count = new LongWritable();
	}

	public VolumeCountPair(LongWritable stockVolume, LongWritable count) 
	{
		super();
		this.stockVolume = stockVolume;
		this.count = count;
	}
	
	public VolumeCountPair(Long stockVolume, Long count) 
	{
		super();
		this.stockVolume = new LongWritable(stockVolume);
		this.count = new LongWritable(count);
	}
	
	public void write(DataOutput out) throws IOException {
		this.stockVolume.write(out);
		this.count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.stockVolume.readFields(in);
		this.count.readFields(in);
	}

	public int compareTo(VolumeCountPair o) {
		int cmp = stockVolume.compareTo(o.stockVolume);
		if (cmp == 0)
			return count.compareTo(o.count);
		return cmp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stockVolume == null) ? 0 : stockVolume.hashCode());
		result = prime * result + ((count == null) ? 0 : count.hashCode());
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
		VolumeCountPair other = (VolumeCountPair) obj;
		if (stockVolume == null) {
			if (other.stockVolume != null)
				return false;
		} else if (!stockVolume.equals(other.stockVolume))
			return false;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return stockVolume + "\t" + count;
	}

}
