package TAM;

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


//Pour chaque station, donner le nombre de trams et bus par jour.

public class NombreTrametBusParStation {

	private static final String INPUT_PATH = "input-TAM_MMM_OffreJour/";
	private static final String OUTPUT_PATH = "output/TAM/NombreTrametBusParStation-";
	private static final Logger LOG = Logger.getLogger(Occitanie.class.getName());

	
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
		
		private final static IntWritable tram = new IntWritable(1);
		private final static IntWritable bus = new IntWritable(2);
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] splited = line.split("\\;");
			
			String stopName = splited[3];
			
			String routeShortName = splited[4];
			
			if(routeShortName.equals("1") || routeShortName.equals("2") || routeShortName.equals("3") || routeShortName.equals("4")) { //case Tram
				context.write(new Text(stopName), tram);
			} else {
				context.write(new Text(stopName), bus);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sumTram = 0;
			int sumBus = 0;

			for (IntWritable val : values)
				if(val.get() == 1)
					sumTram++;
				else
					sumBus++;

			context.write(key, new Text("==> Nombre de tram : " + sumTram + " Nombre de bus : " + sumBus));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "NombreTrametBusParStation");

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
