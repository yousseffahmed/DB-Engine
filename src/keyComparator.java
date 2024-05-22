import java.io.IOException;
import java.util.Comparator;
import java.util.Hashtable;

import com.opencsv.exceptions.CsvValidationException;

public class keyComparator implements Comparator {
	String tblName;
	String addrCSV;
	String clustKey;
	public keyComparator(String tblName,String addrCSV,String clustKey) {
		this.tblName = tblName;
		this.addrCSV = addrCSV;
		this.clustKey = clustKey;
	}
	public int compare(Object o1, Object o2) {
		Hashtable<String,Object> O1 = (Hashtable<String, Object>) o1;
		Hashtable<String,Object> O2 = (Hashtable<String, Object>) o2;
		try {
			return DBApp.compareKeys(tblName, addrCSV, O1.get(clustKey), O2.get(clustKey));
		} catch (CsvValidationException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
}
