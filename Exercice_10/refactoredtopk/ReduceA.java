package refactoredtopk;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


class ReduceA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private TreeMap<Text, Double> sortedLineProfil = new TreeMap<>();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Double sum =  0.0;
		for (DoubleWritable val : values)
			sum += val.get();


		context.write(key , new DoubleWritable(sum));
					
	}
}