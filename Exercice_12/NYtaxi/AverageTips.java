package NYtaxi;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import static java.util.stream.Collectors.*;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Par rapport au prix des courses, quel est le pourcentage du pourboire ?

public class AverageTips {
	
	private static final String INPUT_PATH = "input-NYtaxi/";
	private static final String OUTPUT_PATH = "output/NYtaxi/AverageTips-";
	private static final Logger LOG = Logger.getLogger(AverageTips.class.getName());

	
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, DoubleWritable, IntWritable> {
		
		private final static String emptyWords[] = { "" };
		private final static IntWritable one = new IntWritable(1);
		
		public static boolean isDouble( String str ){
			  try{
			    Double.parseDouble( str );
			    return true;}
			  catch( Exception e ){
			    return false;
			  }
		}
	    
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\\,");

			if (Arrays.equals(splited, emptyWords))
				return;
			
			if(isDouble(splited[13]) && isDouble(splited[16])) {
				Double tips = Double.parseDouble(splited[13]);
				Double total = Double.parseDouble(splited[16]);
				if(total != 0.0) {
					if(tips!=0.0 ) {
					Double ratio = (tips*100)/total;
					context.write(new DoubleWritable(ratio), one);
					}
					else {
						context.write(new DoubleWritable(0), one);
					}
				}
			}		
		}
	}
	
	public static class Reduce extends Reducer<DoubleWritable, IntWritable, Text, Text> {

		private double allRatios = 0.0;
		private int totalBill = 0;
		
		@Override
		public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Double keyCopy = key.get();
			int sum = 0;
			for (IntWritable val : values)
				sum ++;
			
			allRatios += keyCopy*sum;
			totalBill += sum;
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
					System.out.println(allRatios + "&&" + totalBill);
					context.write(new Text("Average tips ratio is equal to ") , 
									new Text(String.format( "%.2f",allRatios/totalBill) + "%"));
		}
	}	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "AverageTips");

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(IntWritable.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}




