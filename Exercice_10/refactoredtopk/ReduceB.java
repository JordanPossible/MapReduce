package refactoredtopk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


class ReduceB extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	
	private LinkedHashMap<Double, List<Text>> sortedLineProfil = new LinkedHashMap<>();
	private int nbsortedWords = 0;
	private int k;

	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Double keyCopy = key.get();
		List tmp = new ArrayList();
		for (Text val : values)
			tmp.add(new Text(val));

		if (sortedLineProfil.containsKey(keyCopy))
			sortedLineProfil.get(keyCopy).addAll(tmp);
		else {
			List<Text> words = new ArrayList<>();

			words.addAll(tmp);
			sortedLineProfil.put(keyCopy, words);
		}
		nbsortedWords+=tmp.size();

		while (nbsortedWords > k) {
			Double firstKey = sortedLineProfil.entrySet().iterator().next().getKey();
			List<Text> lines = sortedLineProfil.get(firstKey);
			lines.remove(lines.size() - 1);
			nbsortedWords--;
			if (lines.isEmpty())
				sortedLineProfil.remove(firstKey);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Double[] nbofs = sortedLineProfil.keySet().toArray(new Double[0]);
		int i = nbofs.length;
		while (i-- != 0) {
			Double nbof = nbofs[i];
			for (Text words : sortedLineProfil.get(nbof))
				context.write(new DoubleWritable(nbof) , words);
		}
	}
}