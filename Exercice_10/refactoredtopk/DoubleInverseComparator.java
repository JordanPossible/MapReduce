package refactoredtopk;

import org.apache.hadoop.io.DoubleWritable;

class DoubleInverseComparator extends InverseComparator<DoubleWritable> {

	public DoubleInverseComparator() {
		super(DoubleWritable.class);
	}
}