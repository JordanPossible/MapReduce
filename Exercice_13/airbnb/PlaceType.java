package airbnb;


import java.util.Arrays;

import java.util.HashMap;

import java.io.IOException;

import java.time.Instant;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Le nombre de chambre et le pourcentage représentatif pour chaque type de logement

public class PlaceType {
	
	private static final String INPUT_PATH = "input-airbnb/";
	private static final String OUTPUT_PATH = "output/airbnb/PlaceType-";
	private static final Logger LOG = Logger.getLogger(PlaceType.class.getName());

	
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

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		
		public static boolean isSharedRoom(String home){
			if(home.equals("Shared room")) return true;
			return false;
		}
		
		public static boolean isPrivateRoom(String home){
			if(home.equals("Private room")) return true;
			return false;
		}
		
		public static boolean isEntireHome(String home){
			if(home.equals("Entire home/apt")) return true;
			return false;
		}

		private final static String emptyWords[] = { "" };
		
		
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\\,");

			if(Arrays.equals(splited, emptyWords))
				return;
			
			if(splited.length == 16){
				String placeType = splited[8];
				if(isEntireHome(placeType)) context.write(new Text(placeType), one);
				if(isPrivateRoom(placeType)) context.write(new Text(placeType), one);
				if(isSharedRoom(placeType)) context.write(new Text(placeType), one);
			}	
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		
		private static HashMap<String , Integer> placeTypeToNumberOfPlace = new HashMap<>();
		private static int totalNumberOfPlace;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable val : values) {
				totalNumberOfPlace++;
				sum++;
			}
			placeTypeToNumberOfPlace.put(key.toString(), sum);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			for(java.util.Map.Entry<String,Integer> entry : placeTypeToNumberOfPlace.entrySet()) {
				  String kkey = entry.getKey();
				  Integer vvalue = entry.getValue();	
				  
				  double pourcent = (vvalue * 100.00)/totalNumberOfPlace;
				  String pourcentAsString = Double.toString(pourcent);
				  context.write(new Text(kkey), new Text(vvalue.toString() + " logements ===> " + pourcentAsString + "%."));
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PlaceType");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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




