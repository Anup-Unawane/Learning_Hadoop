package nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import nyse.keyvalues.DateVolumePair;

public class DateVolumePairComparator extends WritableComparator 
{
	public DateVolumePairComparator() {
		super(DateVolumePair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		DateVolumePair p1 = (DateVolumePair) a;
		DateVolumePair p2 = (DateVolumePair) b;

		int cmp = DateVolumePair.compare(p1.getTradeDate(), p2.getTradeDate());

		//- given to sort in descending order
		if(cmp == 0) {
			cmp = - DateVolumePair.compare(p1.getTradeVolume(), p2.getTradeVolume());
		}
		return cmp;
	}


}
