import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import com.opencsv.exceptions.CsvValidationException;

public class Node implements Serializable{
	ArrayList<Node> children = new ArrayList<>();
	ArrayList<tupleReference> nodeContent = new ArrayList<tupleReference>();
	Object minX;
	Object maxX;
	Object minY;
	Object maxY;
	Object minZ;
	Object maxZ;
	Node parent;
	public Node(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
		this.minZ = minZ;
		this.maxZ = maxZ;
	}
	public static int max() {
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
		int num = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
		return(num);
	 }
	public void insert(tupleReference tr) {
		this.nodeContent.add(tr);
	}
	public boolean isLeaf() {
		if(this.children.size() == 0)
			return true;
		return false;
	}
	public static String MiddleString(String S, String T)
    {
        int N = Math.min(S.length(), T.length());
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;
        }
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }
        for (int i = 0; i <= N; i++) {
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }
            a1[i] = (int)a1[i] / 2;
        }
        String result = "";
        for (int i = 1; i <= N; i++) {
            result+=((char)(a1[i] + 97));
        }
        return result;
    }
	public Object getMidX(String keyType) {
		Object mid = null;
		switch(keyType) {
			case "java.lang.double":
				mid = ((double)this.maxX+(double)this.minX)/2;
				return mid;
			case "java.lang.Integer":
				mid = ((Integer)this.maxX+(Integer)this.minX)/2;
				return mid;
			case "java.lang.String":
				mid = MiddleString((String)this.minX,(String)this.maxX);
				return mid;
			case "java.lang.Date":
				Date max = (Date) this.maxX;
				Date min = (Date) this.minX;
				int hour = (max.getHours() - min.getHours())/2;
				int month = (max.getMonth() - min.getMonth())/2;
				int year = (max.getYear() - min.getYear())/2;
				Calendar cal = Calendar.getInstance();
				cal.setTime(min);
				cal.add(Calendar.HOUR, hour);
				cal.add(Calendar.MONTH, month);
				cal.add(Calendar.YEAR, year);
				return cal.getTime();
		}
		return null;
	}
	public Object getMidY(String keyType) {
		Object mid = null;
		switch(keyType) {
			case "java.lang.double":
				mid = ((double)this.maxY+(double)this.minY)/2;
				return mid;
			case "java.lang.Integer":
				mid = ((Integer)this.maxY+(Integer)this.minY)/2;
				return mid;
			case "java.lang.String":
				mid = MiddleString((String)this.minY,(String)this.maxY);
				return mid;
			case "java.lang.Date":
				Date max = (Date) this.maxY;
				Date min = (Date) this.minY;
				int hour = (max.getHours() - min.getHours())/2;
				int month = (max.getMonth() - min.getMonth())/2;
				int year = (max.getYear() - min.getYear())/2;
				Calendar cal = Calendar.getInstance();
				cal.setTime(min);
				cal.add(Calendar.HOUR, hour);
				cal.add(Calendar.MONTH, month);
				cal.add(Calendar.YEAR, year);
				return cal.getTime();
		}
		return null;
	}
	public Object getMidZ(String keyType) {
		Object mid = null;
		switch(keyType) {
			case "java.lang.double":
				mid = ((double)this.maxZ+(double)this.minZ)/2;
				return mid;
			case "java.lang.Integer":
				mid = ((Integer)this.maxZ+(Integer)this.minZ)/2;
				return mid;
			case "java.lang.String":
				mid = MiddleString((String)this.minZ,(String)this.maxZ);
				return mid;
			case "java.lang.Date":
				Date max = (Date) this.maxZ;
				Date min = (Date) this.minZ;
				int hour = (max.getHours() - min.getHours())/2;
				int month = (max.getMonth() - min.getMonth())/2;
				int year = (max.getYear() - min.getYear())/2;
				Calendar cal = Calendar.getInstance();
				cal.setTime(min);
				cal.add(Calendar.HOUR, hour);
				cal.add(Calendar.MONTH, month);
				cal.add(Calendar.YEAR, year);
				return cal.getTime();
		}
		return null;
	}
	public void createChildren(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ, String typeX, String typeY, String typeZ,Node n) {
		Node child_1 = new Node(minX,getMidX(typeX), minY, getMidY(typeY), minZ, getMidZ(typeZ));
		Node child_2 = new Node(minX,getMidX(typeX), minY, getMidY(typeY), minZ, maxZ);
		Node child_3 = new Node(minX,getMidX(typeX), minY, maxY, minZ, getMidZ(typeZ));
		Node child_4 = new Node(minX,getMidX(typeX), minY, maxY, getMidZ(typeZ), maxZ);
		Node child_5 = new Node(getMidX(typeX), maxX, minY, getMidY(typeY), minZ, getMidZ(typeZ));
		Node child_6 = new Node(getMidX(typeX), maxX, minY, getMidY(typeY), getMidZ(typeZ), maxZ);
		Node child_7 = new Node(getMidX(typeX), maxX, getMidY(typeY), maxY, minZ, getMidZ(typeZ));
		Node child_8 = new Node(getMidX(typeX), maxX, getMidY(typeY), maxY, getMidZ(typeZ), maxZ);
		child_1.parent = n;
		child_2.parent = n;
		child_3.parent = n;
		child_4.parent = n;
		child_5.parent = n;
		child_6.parent = n;
		child_7.parent = n;
		child_7.parent = n;
		this.children.add(child_1);
		this.children.add(child_2);
		this.children.add(child_3);
		this.children.add(child_4);
		this.children.add(child_5);
		this.children.add(child_6);
		this.children.add(child_7);
		this.children.add(child_8);
	}
	public boolean correctPos(Object x, Object y, Object z, String addrTable, String addrCSV, String[] colNames) throws CsvValidationException, IOException {
		boolean xCheck = DBApp.compareKeys(addrTable, addrCSV, colNames[0], x, this.minX) >= 0 && DBApp.compareKeys(addrTable, addrCSV, colNames[0], x, this.maxX) <= 0;
		boolean yCheck = DBApp.compareKeys(addrTable, addrCSV, colNames[1], y, this.minY) >= 0 && DBApp.compareKeys(addrTable, addrCSV, colNames[1], y, this.maxY) <= 0;
		boolean zCheck = DBApp.compareKeys(addrTable, addrCSV, colNames[2], z, this.minZ) >= 0 && DBApp.compareKeys(addrTable, addrCSV, colNames[2], z, this.maxZ) <= 0;
		return xCheck && yCheck && zCheck;
	}
	public String printData() {
		String r="";
		for(int i=0;i<this.nodeContent.size();i++) {
			r+=nodeContent.get(i) + "\n";
		}
		return r;
	}
	public String toString() {
		String r =" X:"+minX+"-" + maxX+ 
				"\n"+
				" Y:"+minY+"-" + maxY+ 
				"\n"+
				" Z:"+minZ+"-" + maxZ+ 
				"\n"+
				this.printData();
				
				
		return r;
	}
}