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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
		private static final Log LOG = LogFactory.getLog(nGramReducer.class);
		
		class Node {

			public String type;
			public int count;
			public Node next;

			public Node(String type, int count) {
				this.type = type;
				this.count = count;
				next = null;
			}
		}

		class LinkedList {
			private Node head;

			public LinkedList() {
				head = null;
			}

			public Node gethead() {
				return head;
			}

			public boolean isEmpty() {
				return head == null;
			}

			public void insert(String type, int count) {
				Node link = new Node(type, count);
				link.next = head;
				head = link;
			}

			public Node delete() {
				Node temp = head;
				head = head.next;
				return temp;
			}

		}
		


		@Override
		protected void reduce(Text keys, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("Entered Reducer class");
			
			Iterator<Text> itr =  values.iterator();
			LinkedList list = new LinkedList();
			String type;
			int count;
			int valueCount = 0;
			boolean isPresent;
			Vector<String> elements = new Vector<String>();
			
			while (itr.hasNext()) {
				Text t = itr.next();
				type = t.toString();
				elements.add(type);
				//System.out.println(elements.get(valueCount));
				valueCount++;
			}
			//String temp1 = keys.toString();
			//System.out.println("Total Number of Values: "+valueCount + " For Key:  "+temp1);
			if (valueCount < 4) {
				String str = keys.toString();
				FinalKey.set(str);
				total.set(0);
				context.write(FinalKey, total);
			}

			if(valueCount >=4) {
				int traverseCount = valueCount - 3;
				int n = 0;

				while (traverseCount > n) {
					int i;
					String interimValue="";
					for (i = n; i < n + 4; i++) {
						interimValue += elements.get(i) + "  ";
					}

					isPresent = false;
					String trimmedValue = interimValue.trim();
					Node tmp = list.gethead();
					
					if (tmp == null) {
						list.insert(trimmedValue, 2);
						
					}

					
					while (tmp != null) {
						if (tmp.type == trimmedValue) {
							tmp.count = tmp.count + 1;
							isPresent = true;
							break;
						}

						tmp = tmp.next;
					}

					if (isPresent == false) {
						list.insert(trimmedValue, 5);
					}

					n++;
				}

				Node temp = list.gethead();
				while (temp != null) {
					String str = keys.toString();
					FinalKey.set(str + "	" + temp.type);
					total.set(temp.count);
					//System.out.println(temp.type + "    " + temp.count);
					//LOG.info(temp.type + "    " + temp.count);
					context.write(FinalKey, total);
					temp = temp.next;
				}
			}
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
