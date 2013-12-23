import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class MovieRecommenderHadoop {


	public static class UserIDMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		
		 public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] s = line.split("\t");
			output.collect(key, new Text(s[0] + "," + s[1]));
		}

	}
	
	public static class ItemPairsReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
	
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			output.collect(key, values.next());
		}
		
	}
	
	
	public static void main(String[] args) {
		
		JobConf conf = new JobConf(MovieRecommenderHadoop.class);
		conf.setJobName("Movie Recommender");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(UserIDMapper.class);
		conf.setReducerClass(ItemPairsReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
