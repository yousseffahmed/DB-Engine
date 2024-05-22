import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class Table implements java.io.Serializable {
	ArrayList<String> tablePages;
	ArrayList<String> tableIndex = new ArrayList<>();
	String TableName;
	boolean hasIndex = false;
	public Table(String tableName) {
		this.TableName = tableName;
		this.tablePages = new ArrayList<String>();
	}
	public static void saveTable(Table table, String addrTable) {
		 try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/Resourses/"+addrTable);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(table);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	 }
	public static Table loadTable(String addrTable) {
		 Table table = null;
	      try {
	         FileInputStream fileIn = new FileInputStream("src/Resourses/"+addrTable);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         table = (Table) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Table not found");
	         c.printStackTrace();
	      }
	      return table;
	 }
	public static void addPage(String addrPage, String addrTable) {
		Table table = loadTable(addrTable);
		table.tablePages.add(addrPage);
		saveTable(table,addrTable);
	}
	public static void deletePage(String addrPage, String addrTable) {
		Table table = loadTable(addrTable);
		table.tablePages.remove(addrPage);
		saveTable(table,addrTable);
	}
	public static void addIndex(String addrIndex, String addrTable) {
		Table table = loadTable(addrTable);
		table.tableIndex.add(addrIndex);
		table.hasIndex = true;
		saveTable(table,addrTable);
	}
}
