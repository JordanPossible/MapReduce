import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
	private static final String INPUT_PATH = "input-join-compact/";
	private static final String OUTPUT_PATH = "output/Join-";
	private static final Logger LOG = Logger.getLogger(Join.class.getName());

	private static HashMap<Integer , String> customerIdtoName = new HashMap<>();
	
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

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public boolean isDouble(String number) {
			try {
				Double.parseDouble(number);
			}catch (NumberFormatException e) {
				return false;
			}
			return true;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splited = line.split("\\|");
			
			if(splited.length == 8){ //customers case
				customerIdtoName.put(Integer.parseInt(splited[0]), splited[1]); //splited[0] contient customer id splited[1] contient customer name
			}else{ //orders case
				if(isDouble(new String(splited[3])))
					context.write(new Text(splited[1]) , new DoubleWritable(Double.parseDouble(splited[3]))); //splited[1] contient orders customer key splited[3] contient total price
			}
		}
	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
	
			try {
				String customerName = customerIdtoName.get(Integer.parseInt(key.toString()));
				Double totalAmount = 0.0;
				for(DoubleWritable val : values) {
					totalAmount += val.get();
				}
				context.write(new Text(customerName),  new DoubleWritable(totalAmount));
			}catch(Exception e) {
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(DoubleWritable.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}