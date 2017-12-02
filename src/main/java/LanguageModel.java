import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration configuration = context.getConfiguration();
			threashold = configuration.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if (count < threashold) {
				return;
			}

			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}
			
			//what is the outputkey?
			String outputkey = sb.toString().trim();
			
			//what is the outputvalue?
			String outputvalue = words[words.length - 1] + "=" + wordsPlusCount[1];
			
			//write key-value to reducer?
			context.write(new Text(outputkey), new Text(outputvalue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			PriorityQueue<Pair> minHeap = new PriorityQueue<>(n, new Comparator<Pair>() {
				@Override
				public int compare(Pair o1, Pair o2) {
					if (o1.equals(o2)) {
						return 0;
					}
					return o1.count < o2.count ? -1 : 1;
				}
			});
			
			for (Text value : values) {
				String val = value.toString().trim();
				String word = val.split("=")[0].trim();
				int count = Integer.parseInt(val.split("=")[1].trim());
				if (minHeap.size() < n) {
					minHeap.offer(new Pair(word, count));
				} else if (minHeap.peek().count < count) {
					minHeap.poll();
					minHeap.offer(new Pair(word, count));
				}
			}

			while (!minHeap.isEmpty()) {
				Pair curr = minHeap.poll();
				context.write(new DBOutputWritable(key.toString(), curr.word, curr.count), NullWritable.get());
			}
		}
	}
}
