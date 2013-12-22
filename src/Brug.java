import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
				System.out.println(s[1] + " " + s[2]);
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
	
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		final String dataFile = "text_data/u.data";
		final String itemFile = "text_data/u.item";
	
		HashMap<Integer, String> map = groupByUser(dataFile);
		
		for (Entry<Integer, String> entry : map.entrySet()) {
		    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
		}
		
	}

}
