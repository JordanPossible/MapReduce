package refactoredtopk;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapB extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public boolean isDouble( String str ){
		  try{
		    Double.parseDouble( str );
		    return true;}
		  catch( Exception e ){
		    return false;
		  }
		}

	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splited = line.split("\\t");
		if (Arrays.equals(splited, emptyWords))
			return;

		if (isDouble(splited[1])){
			context.write(new DoubleWritable(Double.parseDouble(splited[1])), new Text (splited[0]));
		}		
	}
}