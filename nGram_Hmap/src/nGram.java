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

public class nGram extends Configured implements Tool {

	static public class nGramMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		// final private static LongWritable ONE = new LongWritable(1);
		private Text key = new Text();
		private Text value = new Text();

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {

			String line = text.toString();
			String[] lineSplit = line.trim().split("\\s+");

			if (lineSplit.length == 7) {
				String date = lineSplit[0];
				String ngram = lineSplit[2];
				String Src = lineSplit[6];
				String Rcv = lineSplit[5];
				String keyString = date + " " + Src + " " + Rcv;
				String valueString = ngram;
				key.set(keyString);
				value.set(valueString);
				context.write(key, value);
			}
			// else
			// context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);

		}

	}

	static public class nGramReducer extends
			Reducer<Text, Text, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		private Text FinalKey = new Text();

		@Override
        public void reduce(Text key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
            Iterator<Text> itr =  values.iterator();
            HashMap<String,Integer> map1 = new HashMap<String,Integer>();
            LinkedList<String> list = new LinkedList<String>();
            int i,j;
            
            while (itr.hasNext()) {
                Text t = itr.next();
                list.add(t.toString());
            }
            if( list.size()>=4)
            {

                for ( i = 0; i < list.size()-3; i++) 
                {
                    String add4= "";
                    for ( j = i; j < i+4; j++) 
                    {
                        add4= add4 +" "+ list.get(j);                
                    }

                    if (map1.get(add4) == null) {
                        map1.put(add4, 1);
                    }
                    else
                    {
                        int c=map1.get(add4);
                        map1.put(add4, c+1);            
                    }            
                }

                for (String  final1 : map1.keySet()) {
                    total.set(map1.get(final1));
                    FinalKey.set(key.toString()+final1.toString());
                    context.write(FinalKey, total);
                }
            }
            //context.write(key, result);
        }
    }
	/*
	 * Text t = itr.next(); type = t.toString(); isPresent = false;
	 * 
	 * Node tmp = list.gethead();
	 * 
	 * if (tmp == null) { list.insert(type, 1); }
	 * 
	 * 
	 * while (tmp != null) { if (tmp.type == type) { tmp.count++; isPresent =
	 * true; break; }
	 * 
	 * tmp = tmp.next; }
	 * 
	 * if(!isPresent){ list.insert(type, 1); }
	 */

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		Job job = new Job(config, "nGram");
		job.setJarByClass(nGram.class);

		// job.setNumReduceTasks(3);
		job.setMapperClass(nGramMapper.class);
		// job.setCombinerClass(nGramReducer.class);
		job.setReducerClass(nGramReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// TextInputFormat.setInputPaths(job, new Path("input/WordCount.txt"));
		// TextOutputFormat.setOutputPath(job, new Path("output"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new nGram(), args));

	}

}
