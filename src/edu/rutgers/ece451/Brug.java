package edu.rutgers.ece451;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Brug {

	private final static String	PAIRS_TABLE_NAME	= "pairs";
	private final static String	TOP_TABLE_NAME		= "top";
	private final static String	TABLE_FAMILY		= "c";
	private final static String	PAIRS_TABLE_RATING	= "rating";
	private final static String	TOP_TABLE_MOVIE	= "movie";
	private final static String	TOP_TABLE_CORR	= "corr";

	public static class MyTableMapper extends TableMapper<IntWritable, Text> {

		public void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
			// process data for the row from the Result instance.

			DecimalFormat df = new DecimalFormat("#.###");
			double sumXY, sumX, sumY, sumXX, sumYY, n;

			sumXY = 0.0;
			sumX = 0.0;
			sumY = 0.0;
			sumXX = 0.0;
			sumYY = 0.0;
			n = 0.0;
			
			Pattern p = Pattern.compile("\\((.*?)\\)",Pattern.DOTALL);
			Matcher matcher = p.matcher(new String(value.getValue(Bytes.toBytes(TABLE_FAMILY), Bytes.toBytes((PAIRS_TABLE_RATING)))));
			while(matcher.find())
			{
				String splitVal = matcher.group(1);
				String[] s = splitVal.toString().split(",");
				double x = Double.parseDouble(s[0]);
				double y = Double.parseDouble(s[1]);

				sumYY += y * y;
				sumXX += x * x;
				sumXY += x * y;
				sumX += x;
				sumY += y;
				n += 1;
			}

			String[] keys = Bytes.toString(key.get()).split(",");

			if (n > 10) {
				double correlation = (n * sumXY - (sumX * sumY)) / (Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n * sumYY - (sumY * sumY)));
				if ((Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n * sumYY - (sumY * sumY))) == 0)
					correlation = 0;
				context.write(new IntWritable(Integer.parseInt(keys[0].substring(1))), new Text(keys[1].substring(0, keys[1].length() - 1) + "," + df.format(correlation)));
			}
		}
	}

	public static class MyTableReducer extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double maxCorrelation = -2.0;
			String maxItem = "";
			for (Text t : values) {
				String[] s = t.toString().split(",");
				if (Double.parseDouble(s[1]) > maxCorrelation) {
					maxCorrelation = Double.parseDouble(s[1]);
					maxItem = s[0];
				}
			}

			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes(TABLE_FAMILY), Bytes.toBytes(TOP_TABLE_MOVIE), Bytes.toBytes(maxItem));
			put.add(Bytes.toBytes(TABLE_FAMILY), Bytes.toBytes(TOP_TABLE_CORR), Bytes.toBytes(String.valueOf(maxCorrelation)));
			
			context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Brug");
		job.setJarByClass(Brug.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
		scan.addColumn(Bytes.toBytes(TABLE_FAMILY), Bytes.toBytes(PAIRS_TABLE_RATING));
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob( // input HBase
				PAIRS_TABLE_NAME, // table name
				scan, // Scan instance to control CF and attribute selection
				MyTableMapper.class, // mapper
				IntWritable.class, // mapper output key
				Text.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				TOP_TABLE_NAME, // output table
				MyTableReducer.class, // reducer class
				job);
		job.setNumReduceTasks(1);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
