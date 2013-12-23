import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



public class MovieRecommenderHadoop {


	public static class UserIDMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		
		 public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] s = line.split("\t");
			output.collect(key, new Text(s[0] + "," + s[1]));
		}

	}
	
	public static class ItemPairsReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
			
			
		}
		
	}
	
		
		
		
	
	public static void main(String[] args) {
		
		JobConf conf = new JobConf(MovieRecommenderHadoop.class);
		conf.setJobName("Movie Recommender");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(UserIDMapper.class);
		
	}

}
