package NYtaxi;

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Est il vrai que la plupart des courses se font avec deux passagers ?
// pour chaque nombre de passagers possibles : le % de se nombre sur la totalité
public class NombrePassager {
	
	private static final String INPUT_PATH = "input-NYtaxi/";
	private static final String OUTPUT_PATH = "output/NYtaxi/NombrePassager-";
	private static final Logger LOG = Logger.getLogger(NombrePassager.class.getName());

	
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

	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static String emptyWords[] = { "" };

		public static boolean isInteger( String str ){
			  try{
			    Integer.parseInt( str );
			    return true;}
			  catch( Exception e ){
			    return false;
			  }
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			String line = value.toString();
			String[] splited = line.split("\\,");
			
			if (Arrays.equals(splited, emptyWords))
				return;
			
			String passengerCount = splited[3];

			if(isInteger(passengerCount)) {
				int pc = Integer.parseInt(passengerCount);
				
				context.write(new IntWritable(pc), one);

			} 
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {
		
		private static HashMap<String , Integer> numberOfPassengerToTotalRace = new HashMap<>();
		private static int totalRaceNumber;
	
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int totalRaceWithXPassagenre = 0;
			
			for (IntWritable val : values) {
				totalRaceNumber++;
				totalRaceWithXPassagenre++;
			}
			numberOfPassengerToTotalRace.put(key.toString(), totalRaceWithXPassagenre);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			for(java.util.Map.Entry<String,Integer> entry : numberOfPassengerToTotalRace.entrySet()) {
				  String key = entry.getKey();
				  Integer value = entry.getValue();	
				  float pourcent = (value * 100)/totalRaceNumber;
				  String pourcentAsString = Float.toString(pourcent);
				  context.write(new Text(key), new Text(pourcentAsString));
			}
		} 
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "NombrePassager");

		job.setOutputKeyClass(IntWritable.class);
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
