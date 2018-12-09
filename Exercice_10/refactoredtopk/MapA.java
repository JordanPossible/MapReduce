package refactoredtopk;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapA extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public static boolean isDouble( String str ){
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
		String[] splited = line.split("\\,");

		if (Arrays.equals(splited, emptyWords))
			return;

		if (isDouble(splited[20])){ //splited[20] = profit
			context.write(new Text(splited[6]), new DoubleWritable(Double.parseDouble(splited[20]))); //splited[6] = customerName
		}		
	}
}