package refactoredtopk;

import org.apache.hadoop.io.Text;

/**
* Inverseur de la comparaison du type Text.
*/
class TextInverseComparator extends InverseComparator<Text> {

	public TextInverseComparator() {
		super(Text.class);
	}
}