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


//Pour chaque station et chaque heure, afficher une information X_tram correspondant au trafic des trams, 
//avec X_tram="faible" si au plus 8 trams sont prévus (noter qu'une ligne de circulation a deux sens, donc au plus 
//4 trams par heure et sens), X_tram="moyen" si entre 9 et 18 trams sont prévus, et X="fort" pour toute autre valeur. 
//Pour les stations où il a seulement des trams (ou des bus) il faut afficher une seule information. Optionnel : 
//comment peut-on prendre en compte la direction des trams pour donner des informations plus précises ?

public class XTram {

	private static final String INPUT_PATH = "input-TAM_MMM_OffreJour/";
	private static final String OUTPUT_PATH = "output/TAM/XTram-";
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
			String[] siplitedDate = splited[7].split(":");
			String departureTimeHour = siplitedDate[0];
			String routeShortName = splited[4];
			
			if(routeShortName.equals("1") || routeShortName.equals("2") || routeShortName.equals("3") || routeShortName.equals("4")) { //case Tram
				context.write(new Text(stopName + " " + departureTimeHour), tram);
			} else {
				context.write(new Text(stopName + " " + departureTimeHour), bus);
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
				if(val.get() == 1) {
					sumTram++;
				}else {
					sumBus++;
				}
			
			if(sumBus == 0 && sumTram > 0) { //Il y a des tram mais pas de bus
				if(sumTram < 5) {
					context.write(new Text(key.toString()), new Text("X_Tram = faible."));
					return;

				}
				if(sumTram > 4 && sumTram < 9) {
					context.write(new Text(key.toString()), new Text("X_Tram = moyen."));
					return;
				}
				
				context.write(new Text(key.toString()), new Text("X_Tram = fort."));
				return;
			}
			if(sumBus > 0 && sumTram == 0) { //Il y a des bus mais pas des tram
				if(sumBus < 5) {
					context.write(new Text(key.toString()), new Text("X_Bus = faible."));
					return;

				}
				if(sumBus > 4 && sumBus < 9) {
					context.write(new Text(key.toString()), new Text("X_Bus = moyen."));
					return;
				}
				
				context.write(new Text(key.toString()), new Text("X_Bus = fort."));
				return;
			}
			if(sumBus > 0 && sumTram > 0) { //Il y a des bus et des tram
				if(sumBus < 5 && sumTram < 5) { //Bfaible Tfaible
					context.write(new Text(key.toString()), new Text("X_Tram = faible. |=| X_Bus = faible.")); 
					return;
				}
				if(sumBus > 4 && sumBus < 9 && sumTram < 5) { //Bmoyen Tfaible
					context.write(new Text(key.toString()), new Text("X_Tram = faible. |=| X_Bus = moyen.")); 
					return;
				}
				if(sumBus > 8 && sumTram < 5) { //Bfort Tfaible
					context.write(new Text(key.toString()), new Text("X_Tram = faible. |=| X_Bus = fort.")); 
					return;
				}
				if(sumBus < 5 && sumTram > 4 && sumTram < 9) { //Bfaible Tmoyen
					context.write(new Text(key.toString()), new Text("X_Tram = moyen. |=| X_Bus = faible.")); 
					return;
				}
				if(sumBus < 5 && sumTram > 8) { //Bfaible Tfort
					context.write(new Text(key.toString()), new Text("X_Tram = fort. |=| X_Bus = faible.")); 
					return;
				}
				if(sumBus > 4 && sumBus < 9 && sumTram > 4 && sumTram < 9) { //Bmoyen Tmoyen
					context.write(new Text(key.toString()), new Text("X_Tram = moyen. |=| X_Bus = moyen.")); 
					return;
				}
				if(sumBus > 4 && sumBus < 9 && sumTram > 8) { //Bmoyen Tfort
					context.write(new Text(key.toString()), new Text("X_Tram = fort. |=| X_Bus = moyen.")); 
					return;
				}
				if(sumBus > 8 && sumTram > 4 && sumTram < 9) { //Bfort Tmoyen
					context.write(new Text(key.toString()), new Text("X_Tram = moyen. |=| X_Bus = fort.")); 
					return;
				}
				if(sumBus > 8 && sumTram > 8) { //Bfort Tfort
					context.write(new Text(key.toString()), new Text("X_Tram = fort. |=| X_Bus = fort.")); 
					return;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "XTram");

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
