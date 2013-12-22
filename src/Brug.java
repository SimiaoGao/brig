import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map.Entry;


public class Brug {
	
	static HashMap<Integer, String>  groupByUser(String dataFile) {
		
		try {
			BufferedReader in = new BufferedReader(new FileReader(new File(dataFile)));
			
			HashMap<Integer, String> map = new HashMap<Integer, String>();
			
			String nextLine;
			
			while ( (nextLine = in.readLine()) != null) {
				
				String[] s = nextLine.split("\t");
				if ( map.containsKey(Integer.parseInt(s[0]))) {
					map.put(Integer.parseInt(s[0]), map.get(Integer.parseInt(s[0])) + s[1] + "," + s[2] + ",");
				}
				else {
				map.put(Integer.parseInt(s[0]), s[1] + "," + s[2] + ",");
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
	
	
	static HashMap<Integer, String> countRatingsPerUser(HashMap<Integer, String> map) {
		
		HashMap<Integer, String> newMap = new HashMap<Integer, String>();
		
		
		for ( Entry<Integer, String> entry : map.entrySet() ) {
			
			String[] values = entry.getValue().split(",");
			int numRatings = 0;
			int totalRating = 0;
			
			for( int i = 1; i < values.length; i += 2) {
				numRatings++;
				totalRating += Integer.parseInt(values[i]);
			}
			
			newMap.put(entry.getKey(),  numRatings + "," + totalRating + "," + entry.getValue());			
		}
		
		return newMap;
	}
	

	static HashMap<String, String> pairItems(HashMap<Integer, String> map) {

		HashMap<String, String> newMap = new HashMap<String, String>();
		
		for (Entry<Integer, String> entry : map.entrySet()) {
			
			String[] item1 = entry.getValue().split(",");
			String[] item2 = entry.getValue().split(",");
			
						
			for( int i = 2; i < item1.length; i+=2) {
				for( int j = 2; j < item2.length; j+=2) {
					
					if(i != j) {
						String key = item1[i] + "," + item2[j];
						if(newMap.containsKey(key)) {
							newMap.put(key, newMap.get(key) + item1[i+1] + "," + item2[j+1] + ",");
						}
						else	{			
						newMap.put(key, item1[i+1] + "," + item2[j+1] + ",");
						}
					}
				}
			}
		}
		
		return newMap;
	}
	
	
	static HashMap<String, String> createCorrelations(HashMap<String, String> map) {
		
		HashMap<String, String> newMap = new HashMap<String, String>();
		
		double sumXY, sumX, sumY, sumXX, sumYY, n;
		
			
		for (Entry<String, String> entry : map.entrySet()) {
			sumXY = 0.0; sumX = 0.0; sumY = 0.0; sumXX = 0.0; sumYY = 0.0; n = 0.0;
			
			String[] s = entry.getValue().split(",");
			
			for( int i =0; i < s.length; i +=2) {
				
				double x = Double.parseDouble(s[i]);
				double y = Double.parseDouble(s[i+1]);
				
				sumYY += y*y;
				sumXX += x*x;
				sumXY += x*y;
				sumX += x;
				sumY += y;
				n++;
			}
			
	

			double correlation = (n*sumXY - sumX*sumY) / ( Math.sqrt(n*sumXX - (sumX*sumX))*Math.sqrt(n*sumYY - (sumY*sumY)));
			if(( Math.sqrt(n*sumXX - (sumX*sumX))*Math.sqrt(n*sumYY - (sumY*sumY))) == 0)
				correlation = 0;
				newMap.put(entry.getKey(), correlation + "," + n);
		}
		
		return newMap;
	}
	
	
	
	static HashMap<String, String> rankItems(HashMap<String, String> map) {
		
		HashMap<String, String> newMap = new HashMap<String, String>();
		
		for (Entry<String, String> entry : map.entrySet()) {
			
			String[] key = entry.getKey().split(",");
			String[] values = entry.getValue().split(",");
			
			newMap.put(key[0] + "," + values[0], key[1] + "," + values[1]);
		}
		
		return newMap;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		final String dataFile = "text_data/u.data";
		final String itemFile = "text_data/u.item";
	
		HashMap<Integer, String> groupByUserMap = groupByUser(dataFile);
		HashMap<Integer, String> addRatingsCountMap = countRatingsPerUser(groupByUserMap);
		HashMap<String, String> pairItemsMap = pairItems(addRatingsCountMap);
		HashMap<String, String> correlationMap = createCorrelations(pairItemsMap);
		HashMap<String, String> rankingMap = rankItems(correlationMap);
		
		PrintWriter writer;
		try {
			writer = new PrintWriter("output.txt", "UTF-8");
			for (Entry<String, String> entry : rankingMap.entrySet()) {
				
			    writer.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
			}
			writer.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		
	}

}
