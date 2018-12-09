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

//Quelles sont les heures de pointe ?
//Les 5 heures de pointes les plus importante avec le nombre de course effectué

public class HeureDePointe {
	
	private static final String INPUT_PATH = "input-NYtaxi/";
	private static final String OUTPUT_PATH = "output/NYtaxi/HeureDePointe-";
	private static final Logger LOG = Logger.getLogger(HeureDePointe.class.getName());

	
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
		private final static String emptyWords[] = { "" };
		
	    public static boolean isValidDate(String date) {
	        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        dateFormat.setLenient(false);
	        try {
	            dateFormat.parse(date.trim());
	        } catch (ParseException pe) {
	            return false;
	        }
	        return true;
	    }

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
			String line = value.toString();
			String[] splited = line.split("\\,");
			
			if (Arrays.equals(splited, emptyWords))
				return;
			if (isValidDate(splited[1])){
				
				String [] splitDateDT = splited[1].split("\\s+");
				
				String[] pickupTimeSplited = splitDateDT[1].split(":");
				String pickupHour = pickupTimeSplited[0];
				
				context.write(new Text(pickupHour), one);
			}		
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private static HashMap<String , Integer> hoursToSumTaxiPerHour = new HashMap<>();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sumTaxiPerHour = 0;

			for (IntWritable val : values)
				sumTaxiPerHour++;
			
			hoursToSumTaxiPerHour.put(key.toString(), sumTaxiPerHour);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			java.util.Map<String, Integer> sorted = hoursToSumTaxiPerHour
					.entrySet()
			        .stream()
			        .sorted(Collections.reverseOrder(java.util.Map.Entry.comparingByValue()))
			        .collect(
			            toMap(java.util.Map.Entry::getKey, java.util.Map.Entry::getValue, (e1, e2) -> e2,
			                LinkedHashMap::new));

			
			int i = 0;
			for(Entry<String , Integer> entry : sorted.entrySet()) {
				if(i < 5) {
					String key = entry.getKey();
					Integer value = entry.getValue();
					context.write(new Text(key) , new IntWritable(value));
					i++;
				} else {
					break;
				}
			}
		} 
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "HeureDePointe");

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




