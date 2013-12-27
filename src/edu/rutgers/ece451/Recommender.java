package edu.rutgers.ece451;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

public class Recommender {

	static HashMap<Integer, String> groupUserID(String dataFile) {

		try {
			BufferedReader in = new BufferedReader(new FileReader(new File(
					dataFile)));

			HashMap<Integer, String> map = new HashMap<Integer, String>();

			String nextLine;

			while ((nextLine = in.readLine()) != null) {

				String[] s = nextLine.split("\t");
				int key = Integer.parseInt(s[0]);

				if (map.containsKey(key)) {
					String currList = map.get(key);
					currList += "," + Integer.parseInt(s[1]) + ","
							+ Integer.parseInt(s[2]);
					map.put(key, currList);
				} else {
					map.put(key,
							Integer.parseInt(s[1]) + ","
									+ Integer.parseInt(s[2]));
				}
			}
			in.close();

			return map;

		} catch (FileNotFoundException e) {
			System.out.println("File Not Found.");
		} catch (IOException e) {
			System.out.println("Could not read File.");
		}

		return null;
	}

	static HashMap<String, String> pairRatingsPerUser(
			HashMap<Integer, String> map) {

		HashMap<String, String> pairs = new HashMap<String, String>();

		for (Entry<Integer, String> entry : map.entrySet()) {

			String[] item1 = entry.getValue().split(",");
			String[] item2 = entry.getValue().split(",");

			for (int i = 0; i < item1.length; i += 2) {
				for (int j = 0; j < item1.length; j += 2) {

					if (i != j) {

						String key = item1[i] + "," + item2[j];

						if (pairs.containsKey(key)) {
							pairs.put(key, pairs.get(key) + "," + item1[i + 1]
									+ "," + item2[j + 1]);
						} else {
							pairs.put(key, item1[i + 1] + "," + item2[j + 1]);
						}

					}
				}
			}

		}

		return pairs;
	}

	static HashMap<String, String> itemCorrelations(HashMap<String, String> map) {

		HashMap<String, String> correlations = new HashMap<String, String>();

		DecimalFormat df = new DecimalFormat("#.###");

		double sumXY, sumX, sumY, sumXX, sumYY, n;

		for (Entry<String, String> entry : map.entrySet()) {
			sumXY = 0.0;
			sumX = 0.0;
			sumY = 0.0;
			sumXX = 0.0;
			sumYY = 0.0;
			n = 0.0;

			String[] s = entry.getValue().split(",");

			for (int i = 0; i < s.length; i += 2) {

				double x = Double.parseDouble(s[i]);
				double y = Double.parseDouble(s[i + 1]);

				sumYY += y * y;
				sumXX += x * x;
				sumXY += x * y;
				sumX += x;
				sumY += y;
				n+=1;
			}

			if (n > 9) {
				double correlation = (n * sumXY - (sumX * sumY))
						/ (Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n
								* sumYY - (sumY * sumY)));
				if ((Math.sqrt(n * sumXX - (sumX * sumX)) * Math.sqrt(n * sumYY
						- (sumY * sumY))) == 0)
					correlation = 0;
				correlations.put(entry.getKey(), df.format(correlation) + ","
						+ n);
			}

		}
		return correlations;
	}

	static HashMap<Integer, String> getRankings(HashMap<String, String> map) {

		HashMap<Integer, String> rankings = new HashMap<Integer, String>();

		for (Entry<String, String> entry : map.entrySet()) {

			String[] keys = entry.getKey().split(",");
			String[] values = entry.getValue().split(",");

			int key = Integer.parseInt(keys[0]);

			if (rankings.containsKey(key)) {
				rankings.put(key, rankings.get(key) + "," + values[0] + ","
						+ keys[1] + "," + values[1]);
			} else {
				rankings.put(key, values[0] + "," + keys[1] + "," + values[1]);
			}

		}
		return rankings;

	}

	public static void main(String[] args) {

		final String dataFile = "text_data/u.data";
		final String itemFile = "text_data/u.item";
		
		long startTime = System.currentTimeMillis();
		
		HashMap<Integer, String> userRatingMap = groupUserID(dataFile);
		HashMap<String, String> pairs = pairRatingsPerUser(userRatingMap);
		HashMap<String, String> correlations = itemCorrelations(pairs);
		HashMap<Integer, String> rankings = getRankings(correlations);

		long endTime = System.currentTimeMillis();
		
		System.out.println("That took " + (endTime - startTime) + " milliseconds");
		
		System.out.println(rankings.get(50));
		/*
		//PrintWriter writer;
		try {
			//writer = new PrintWriter("output.txt", "UTF-8");

			//writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		*/

	}

}
