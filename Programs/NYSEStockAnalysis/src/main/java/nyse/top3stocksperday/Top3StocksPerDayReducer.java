package nyse.top3stocksperday;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import nyse.keyvalues.DateVolumePair;

public class Top3StocksPerDayReducer extends Reducer<DateVolumePair, Text, DateVolumePair, Text> {

	@Override
	protected void reduce(DateVolumePair key, Iterable<Text> records,
			Reducer<DateVolumePair, Text, DateVolumePair, Text>.Context context) throws IOException, InterruptedException {
		int count = 0;
		for(Text t : records)
		{
			if(count < 3)
				context.write(key, t);
			else
				break;
			count++;
		}
	}
}
