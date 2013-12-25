package edu.rutgers.ece451;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Credits to: 
// http://sujitpal.blogspot.com/2012/07/fun-with-hadoop-in-action-exercises-java.html
// http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html

public class MovieRecommenderHadoop extends Configured implements Tool {

	public static class UserIDMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {

			String[] s = value.toString().split("\t");
			ctx.write(new IntWritable(Integer.parseInt(s[0])), new Text(s[1]
					+ "," + s[2]));
		}
	}

	public static class PairsReducer extends
			Reducer<IntWritable, Text, Text, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context ctx) throws IOException, InterruptedException {

			String concat = StringUtils.join(values.iterator(), ",");

			String[] item1 = concat.split(",");
			String[] item2 = concat.split(",");

			for (int i = 0; i < item1.length; i += 2) {
				for (int j = 0; j < item1.length; j += 2) {

					if (i != j) {

						ctx.write(new Text(item1[i] + "," + item2[j]),
								new Text(item1[i + 1] + "," + item2[j + 1]));

					}
				}
			}

		}
	}

	public static class IdentityMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {

			String[] s = value.toString().split("\t");
			ctx.write(new Text(s[0]), new Text(s[1]));
		}
	}
	
	public static class AllPairsCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			
			String concat = StringUtils.join(values.iterator(), ",");
			
			ctx.write(key, new Text(concat));
		}
	}
	

	public static class AllPairsReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {

			String concat = StringUtils.join(values.iterator(), ",");

			ctx.write(key, new Text(concat));
		}
	}

	public static class CorrelationMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {

			DecimalFormat df = new DecimalFormat("#.###");
			double sumXY, sumX, sumY, sumXX, sumYY, n;

			sumXY = 0.0;
			sumX = 0.0;
			sumY = 0.0;
			sumXX = 0.0;
			sumYY = 0.0;
			n = 0.0;

			String[] splitVal = value.toString().split("\t");

			String[] keys = splitVal[0].toString().split(",");
			String[] s = splitVal[1].toString().split(",");

			for (int i = 0; i < s.length; i += 2) {

				double x = Double.parseDouble(s[i]);
				double y = Double.parseDouble(s[i + 1]);

				sumYY += y * y;
				sumXX += x * x;
				sumXY += x * y;
				sumX += x;
				sumY += y;
				n += 1;
			}

			if (n > 50) {
				double correlation = (n * sumXY - (sumX * sumY))
						/ (Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n
								* sumYY - (sumY * sumY)));
				if ((Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n * sumYY
						- (sumY * sumY))) == 0)
					correlation = 0;
				ctx.write(new Text(keys[0]),
						new Text(keys[1] + "," + df.format(correlation)));
				// ctx.write(new Text(keys[0]), new Text(keys[1] + "," +
				// df.format(correlation) + "," + n));
			}

		}
	}

	public static class MaxCorrelationReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {

			double maxCorrelation = -2.0;
			String maxItem = "";
			for (Text t : values) {
				String[] s = t.toString().split(",");
				if (Double.parseDouble(s[1]) > maxCorrelation) {
					maxCorrelation = Double.parseDouble(s[1]);
					maxItem = s[0];
				}
			}

			ctx.write(key, new Text(maxItem + "," + maxCorrelation));

		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf,
				"Movie Recommender - Group By User + Pairwise calculations");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("temp1"));

		job.setJarByClass(MovieRecommenderHadoop.class);
		job.setMapperClass(UserIDMapper.class);
		job.setReducerClass(PairsReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			System.out
					.println("Group By User + Pairwise calculations failed, exiting");
			return -1;
		}

		Job temp = new Job(conf, "Movie Recommender - Group by Pair");
		FileInputFormat.addInputPath(temp, new Path("temp1"));
		FileOutputFormat.setOutputPath(temp, new Path("temp2"));

		temp.setJarByClass(MovieRecommenderHadoop.class);
		temp.setMapperClass(IdentityMapper.class);
		temp.setCombinerClass(AllPairsCombiner.class);
		temp.setReducerClass(AllPairsReducer.class);
		temp.setInputFormatClass(TextInputFormat.class);
		temp.setMapOutputKeyClass(Text.class);
		temp.setMapOutputValueClass(Text.class);
		temp.setOutputKeyClass(Text.class);
		temp.setOutputValueClass(Text.class);

		succ = temp.waitForCompletion(true);
		if (!succ) {
			System.out.println("Group by Pair temp failed, exiting");
			return -1;
		}

		Job job2 = new Job(conf,
				"Movie Recommender - Correlation calculations + Max Correlation per Movie");
		FileInputFormat.addInputPath(job2, new Path("temp2"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setJarByClass(MovieRecommenderHadoop.class);
		job2.setMapperClass(CorrelationMapper.class);
		job2.setReducerClass(MaxCorrelationReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out
					.println("Correlation calculations + Max Correlation per Movie failed, exiting");
			return -1;
		}

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("temp1"), true);
		fs.delete(new Path("temp2"), true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MovieRecommenderHadoop(), args);
		System.exit(res);
	}

}
