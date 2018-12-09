package NYtaxi;

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
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


//Quel est le moyen de paiement le plus utilisé par jour ?
public class MoyenPaimentParJour {
	
	private static final String INPUT_PATH = "input-NYtaxi/";
	private static final String OUTPUT_PATH = "output/NYtaxi/MoyenPaimentParJour-";
	private static final Logger LOG = Logger.getLogger(MoyenPaimentParJour.class.getName());

	
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
		
		private final static IntWritable creditCard = new IntWritable(1);
		private final static IntWritable cash = new IntWritable(2);
		private final static IntWritable noCharge = new IntWritable(3);
		private final static IntWritable dispute = new IntWritable(4);
		private final static IntWritable unknown = new IntWritable(5);
		private final static IntWritable voidedTrip = new IntWritable(6);
		
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
			
			String paymentType = splited[9];
			
			if (isValidDate(splited[1])){
				String [] splitDateDT = splited[1].split("\\s+");
				
				String[] pickupTimeSplited = splitDateDT[0].split("-");

				
				String day = pickupTimeSplited[2];

				switch(paymentType) {
				case "1" :
					context.write(new Text(day), creditCard);
					break;
				case "2" :
					context.write(new Text(day), cash);
					break;
				case "3" :
					context.write(new Text(day), noCharge);
					break;
				case "4" :
					context.write(new Text(day), dispute);
					break;
				case "5" :
					context.write(new Text(day), unknown);
					break;
				case "6" :
					context.write(new Text(day), voidedTrip);
					break;
				}
			}		
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		
		private static TreeMap<Integer , Integer> paymentRecord = new TreeMap<>();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sumCreditCard = 0;
			int sumcash = 0;
			int sumNoCharge = 0;
			int sumDispute = 0;
			int sumUnknown = 0;
			int sumVoidedTrip = 0;

			for (IntWritable val : values) {
				switch(val.get()){
					case 1 :
						sumCreditCard++;
						break;
					case 2 :
						sumcash++;
						break;
					case 3 :
						sumNoCharge++;
						break;
					case 4 :
						sumDispute++;
						break;
					case 5 :
						sumUnknown++;
						break;
					case 6 :
						sumVoidedTrip++;
						break;
				}
			}
			
//			LOG.info("sumCreditCard : " + sumCreditCard);
//			LOG.info("sumcash : " + sumcash);
//			LOG.info("sumNoCharge : " + sumNoCharge);
//			LOG.info("sumDispute : " + sumDispute);
//			LOG.info("sumUnknown : " + sumUnknown);
//			LOG.info("sumVoidedTrip : " + sumVoidedTrip);
//			LOG.info("---------------------------------------");
			
			paymentRecord.put(sumCreditCard, 1);
			paymentRecord.put(sumcash, 2);
			paymentRecord.put(sumNoCharge, 3);
			paymentRecord.put(sumDispute, 4);
			paymentRecord.put(sumUnknown, 5);
			paymentRecord.put(sumVoidedTrip, 6);

			int paymentTypeMostUsed = paymentRecord.get(paymentRecord.lastKey());
			
			switch(paymentTypeMostUsed) {
			case 1:
				context.write(new Text(key.toString()), new Text("Credit Card"));
				break;
			case 2:
				context.write(new Text(key.toString()), new Text("Cash"));
				break;
			case 3:
				context.write(new Text(key.toString()), new Text("No Charge"));
				break;
			case 4:
				context.write(new Text(key.toString()), new Text("Dispute"));
				break;
			case 5:
				context.write(new Text(key.toString()), new Text("Unknow"));
				break;
			case 6:
				context.write(new Text(key.toString()), new Text("Voided Trip"));
				break;
			}
			
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MoyenPaimentParJour");

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
