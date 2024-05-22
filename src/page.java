import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class page extends Vector implements java.io.Serializable {
	String addrPage;
	public page(String addrPage) {
		 super();	
		 this.addrPage = addrPage;
	    }
	public static int rows() {
		 Properties prop=new Properties();
		 FileInputStream ip;
		try {
			ip = new FileInputStream("src/Resourses/DBApp.config");
			prop.load(ip);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		int num = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
		return(num);
	 }
	 public static Hashtable<String,Object> min(String addrPage, String addrTable) {
		 page Page = loadPage(addrPage,addrTable);
		 return (Hashtable<String, Object>) (Page.get(0));
	 }
	 public static Hashtable<String,Object> max(String addrPage, String addrTable) {
		 page Page = loadPage(addrPage,addrTable);
		 return (Hashtable<String, Object>) (Page.get(Page.size()-1));
	 }
	 public static void deletePage(String addrPage, String addrTable) {
		 File pageFile = new File("src/Resourses/"+addrTable+addrPage);
		 pageFile.delete();
	 }
	 public static void savePage(page Page, String addrPage,String addrTable) {
		 try {
	         FileOutputStream fileOut =
	         new FileOutputStream("src/Resourses/"+addrTable+addrPage);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(Page);
	         out.close();
	         fileOut.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	 }
	 public static page loadPage(String addrPage,String addrTable) {

		 page Page = new page(addrPage);
	      try {
	         FileInputStream fileIn = new FileInputStream("src/Resourses/"+addrTable+addrPage);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         Page = (page) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	      } catch (ClassNotFoundException c) {
	         System.out.println("Page not found");
	         c.printStackTrace();
	      }
	      return Page;
	 }
	 public static void insertIntoPage(Hashtable<String, Object> Tuple,String addrPage,String tblName,String addrCSV,String clustKey) {
		 page Page = page.loadPage(addrPage,tblName);
		 Page.add(Tuple);
		 Collections.sort(Page,new keyComparator(tblName, addrCSV, clustKey));
		 savePage(Page,addrPage,tblName); 
	 }
	 public static void deleteFromPage(Hashtable<String, Object> Tuple, String addrPage, String tblName) throws DBAppException {
		 page Page = loadPage(addrPage,tblName);
		 int size = Page.size();
		 ArrayList<Hashtable<String, Object>> Elements = new ArrayList<>();
		 for(int i = 0; i < size;i++) {
			 Enumeration<String>colName = Tuple.keys();
			 Hashtable<String, Object> pageTuple = (Hashtable<String, Object>) Page.get(i);
			 Boolean flag = false;
			 while(colName.hasMoreElements()) {
				 String ColName = colName.nextElement();
				 if(Tuple.get(ColName).equals(pageTuple.get(ColName)))
					flag = true;
				 else {
					 flag = false;
					 break;
				 }
			 }
			 if(flag)
				 Elements.add((Hashtable<String, Object>) Page.get(i));
		 }	 
		 for(int i = 0; i < Elements.size();i++) {
			 Page.remove(Elements.get(i));
		 }
		 savePage(Page,addrPage,tblName);
	 }
}
