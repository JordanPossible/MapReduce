package NYtaxi;


import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
import java.time.Instant;
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

// Quel est le prix prix moyen / médian d'une course ?
public class AverageMedianPrice {
	
	private static final String INPUT_PATH = "input-NYtaxi/";
	private static final String OUTPUT_PATH = "output/NYtaxi/AverageMedianPrice-";
	private static final Logger LOG = Logger.getLogger(AverageMedianPrice.class.getName());

	
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
			
			if(isDouble(splited[4])) {
				Double price = Double.parseDouble(splited[16]);

				context.write(new DoubleWritable(price), one);
			} 
		}
	
	public static class Reduce extends Reducer<DoubleWritable, IntWritable, Text, Text> {

		private ArrayList <Double> allPrices = new ArrayList<>();
		
		@Override
		public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Double keyCopy = key.get();
			
			for (IntWritable val : values)
				allPrices.add(keyCopy);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Double totalPrices = 0.0;
			for(Double price: allPrices ) {
				totalPrices+= price;
			}
			Double averagePrices = totalPrices/allPrices.size();
			
			double medianPrice;
			if(allPrices.size() % 2 ==0 ) {	
				medianPrice = (allPrices.get(allPrices.size()/2) + allPrices.get(allPrices.size()/2+1))/2;
			}else {	
				medianPrice = allPrices.get(allPrices.size()/2+1);
			}
			context.write(new Text(" Median Price : " + String.format( "%.2f",medianPrice) + " Dollars") , 
							new Text(" Average Price : " + String.format( "%.2f",averagePrices) + " Dollars"));
			
		}
	}	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "AverageMedianPrice");

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
}




