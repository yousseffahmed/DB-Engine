import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import com.opencsv.exceptions.CsvValidationException;

public class Index implements java.io.Serializable{
	Octree oct;
	String [] colNames;
	public Index() {
		
	}
	public Index(Octree oct, String [] colNames) {
		this.oct = oct;
		this.colNames = colNames;
	}
	public static void saveIndex(Index index, String addrIndex) {
		 try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/Resourses/"+addrIndex);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(index);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	 }
	public static Index loadIndex(String addrIndex) {
		 Index index = new Index();
	      try {
	         FileInputStream fileIn = new FileInputStream("src/Resourses/"+ addrIndex);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         index = (Index) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Index not found");
	         c.printStackTrace();
	      }
	      return index;
	 }
	public static void insert(String addrIndex, tupleReference tr) throws CsvValidationException, IOException {
		Index index = loadIndex(addrIndex);
		index.oct.insert(tr);
		saveIndex(index, addrIndex);
	}
	public static void delete(String addrIndex, tupleReference tr) throws CsvValidationException, IOException {
		Index index = loadIndex(addrIndex);
		index.oct.delete(tr);
		saveIndex(index, addrIndex);
	}
	public static String search(String addrIndex, tupleReference tr) throws CsvValidationException, IOException {
		Index index = loadIndex(addrIndex);
		return(index.oct.search(tr));
	}
//	public static ArrayList<String> searchSelect(String addrIndex, tupleReference tr, String[] op) throws CsvValidationException, IOException {
//		ArrayList<String> result = new ArrayList<String>();
//		Index index = loadIndex(addrIndex);
//		result = index.oct.searchSelect(tr, op);
//		return(result);
//	}
	public static void printIndex(String addrIndex) {
		Index index = loadIndex(addrIndex);
		index.oct.printOctree();
		saveIndex(index,addrIndex);
	}
	public static ArrayList<Node> searchSelect(Object x, Object y, Object z, String [] op, String addrIndex){
			ArrayList<Node> res = new ArrayList<Node>();
			Index index  = loadIndex(addrIndex);
			res = index.oct.searchSelect(x, y, z, op, null);
			return res;
	}
}