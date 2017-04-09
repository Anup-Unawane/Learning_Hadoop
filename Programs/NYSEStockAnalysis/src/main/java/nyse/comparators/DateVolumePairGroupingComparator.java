package nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import nyse.keyvalues.DateVolumePair;

public class DateVolumePairGroupingComparator extends WritableComparator 
{
	public DateVolumePairGroupingComparator() {
		super(DateVolumePair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateVolumePair p1 = (DateVolumePair) a;
		DateVolumePair p2 = (DateVolumePair) b;

		return DateVolumePair.compare(p1.getTradeDate(), p2.getTradeDate());

	}
}
