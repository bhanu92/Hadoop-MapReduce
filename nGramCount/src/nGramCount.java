import java.io.*;
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class nGramCount extends Configured implements Tool {

	static public class nGramCountMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		// final private static LongWritable ONE = new LongWritable(1);
		private Text key = new Text();
		private LongWritable value = new LongWritable();
		//private Text value = new Text();
		//final private static LongWritable ONE = new LongWritable(1);

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {

			String line = text.toString();
			String[] lineSplit = line.trim().split("\\s+");
			
			if(lineSplit.length == 8){
				int i;
				String keyString="";
				for(i=3; i<7; i++){
					keyString += lineSplit[i]+ "  ";
				}
				keyString = keyString.trim();
				keyString = lineSplit[0]+"  "+keyString;
				key.set(keyString);
				int val = Integer.parseInt(lineSplit[7]);
				value.set(val);
				context.write(key, value );
			}

		}

	}

	static public class nGramCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
	      private LongWritable total = new LongWritable();

	      @Override
	      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
	            throws IOException, InterruptedException {
	         long n = 0;
	         for (LongWritable count : counts)
	            n += count.get();
	         total.set(n);
	         context.write(token, total);
	      }
	}


	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Job job = new Job(config, "nGram");
		job.setJarByClass(nGramCount.class);

		// job.setNumReduceTasks(3);
		job.setMapperClass(nGramCountMapper.class);
		// job.setCombinerClass(nGramReducer.class);
		job.setReducerClass(nGramCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// TextInputFormat.setInputPaths(job, new Path("input/WordCount.txt"));
		// TextOutputFormat.setOutputPath(job, new Path("output"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new nGramCount(), args));

	}

}
