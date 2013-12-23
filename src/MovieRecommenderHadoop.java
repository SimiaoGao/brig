import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class MovieRecommenderHadoop extends Configured implements Tool {

	public static class Mapper1 extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
		
			String[] s = value.toString().split("\t");
			ctx.write(new IntWritable(Integer.parseInt(s[0])), new Text(s[1]
					+ "," + s[2]));
		}
	}

	
	
	public static class Combiner1 extends
			Reducer<IntWritable, Text, Text, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context ctx) throws IOException, InterruptedException {

			String[] item1 = values.toString().split(",");
			String[] item2 = values.toString().split(",");

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
	

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			
			System.out.println(values.toString());

			DecimalFormat df = new DecimalFormat("#.###");
			double sumXY, sumX, sumY, sumXX, sumYY, n;

			for (Text t : values) {

				sumXY = 0.0;
				sumX = 0.0;
				sumY = 0.0;
				sumXX = 0.0;
				sumYY = 0.0;
				n = 0.0;
				String[] s = t.toString().split(",");

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
							/ (Math.sqrt(n * sumXX - (sumX * sumX)) * Math
									.sqrt(n * sumYY - (sumY * sumY)));
					if ((Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n
							* sumYY - (sumY * sumY))) == 0)
						correlation = 0;
					ctx.write(key, new Text(df.format(correlation) + "," + n));
				}

			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Movie Recommender");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(MovieRecommenderHadoop.class);
		job.setMapperClass(Mapper1.class);
		//job.setCombinerClass(Combiner1.class);
		job.setReducerClass(Reducer1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job failed, exiting");
			return -1;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MovieRecommenderHadoop(), args);
		System.exit(res);
	}

}
