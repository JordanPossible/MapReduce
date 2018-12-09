package airbnb;


import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.stream.Collectors.toMap;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// calculer le taux de disponibilité de chaque annonce de 
//logements entiers et afficher les résultats par ordre décroissant.

public class TauxOccupation {
	
	private static final String INPUT_PATH = "input-airbnb/";
	private static final String OUTPUT_PATH = "output/airbnb/TauxOccupation-";
	private static final Logger LOG = Logger.getLogger(TauxOccupation.class.getName());

	
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

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		public static boolean isInteger(String number){
			  try{
			    Integer.parseInt(number);
			    return true;}
			  catch(Exception e){
			    return false;
			  }
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
				if(isInteger(splited[15]) &&  isEntireHome(splited[8])){
					String aivaibilty365 = splited[15];
					String id = splited[0];
					context.write(new Text(id), new Text(aivaibilty365));
				}
			}	
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		
		static <K,V extends Comparable<? super V>>
		SortedSet<java.util.Map.Entry<K,V>> entriesSortedByValues(java.util.Map<K,V> map) {
		    SortedSet<java.util.Map.Entry<K,V>> sortedEntries = new TreeSet<java.util.Map.Entry<K,V>>(
		        new Comparator<java.util.Map.Entry<K,V>>() {
		            @Override public int compare(java.util.Map.Entry<K,V> e1, java.util.Map.Entry<K,V> e2) {
		                int res = e1.getValue().compareTo(e2.getValue());
		                return res != 0 ? res : 1;
		            }
		        }
		    );
		    sortedEntries.addAll(map.entrySet());
		    return sortedEntries;
		}
		
		private static TreeMap<String , Double> IDtoOccupationRatio = new TreeMap<>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Double ratio = 00.00;
			
			for (Text val : values) {
				ratio = Double.parseDouble(val.toString());
				ratio = ratio/365.00;
			}

			IDtoOccupationRatio.put(key.toString(), ratio);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			int i = 0;
			for (Entry<String, Double> entry  : entriesSortedByValues(IDtoOccupationRatio)) {
				if(i < 5000) {
					String key = entry.getKey();
					Double value = entry.getValue();
					context.write(new Text("ID : " + key), new DoubleWritable(value));
					i++;
				} else {
					break;
				}
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "TauxOccupation");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(Text.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}




