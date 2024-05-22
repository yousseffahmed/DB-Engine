import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import javax.naming.spi.DirStateFactory.Result;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

public class DBApp {
	static Hashtable<String, String> htblColNameType = new Hashtable<String, String>(); 
	static ArrayList<String> colType = new ArrayList<String>();
	static ArrayList<Integer> colMax = new ArrayList<Integer>();
	static ArrayList<Integer> colMin = new ArrayList<Integer>();
	static ArrayList<String> colKeys = new ArrayList<String>();
	static ArrayList<String> clstrArray = new ArrayList<String>();
	static ArrayList<entry> entries = new ArrayList<entry>();
	static ArrayList<String> colIndex = new ArrayList<String>();
	public static Hashtable<String,String> indexNameHash = new Hashtable<String,String>();
	public static Hashtable<String,String> indexTypeHash = new Hashtable<String,String>();
    private static final String CSV_SEPARATOR = ",";
    public static String addrCSV = "metadata.csv";
    public static Table table;
    public static page Page;
    public static String addrIndex;
	public void init() {
	}
	public void createTable(String strTableName, String strClusteringKeyColumn, 
			Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax )  throws DBAppException{
		File file = new File("src/Resourses/"+strTableName);
		if(file.exists())
			throw new DBAppException("Table Already Exists");
		Enumeration<String> keys = htblColNameType.keys();
		Boolean flagClust = false;
		while(keys.hasMoreElements()) {
			String colName = keys.nextElement();
			String colType = htblColNameType.get(colName);
			Boolean flagType = false;
			if(colName.equals(strClusteringKeyColumn))
				flagClust = true;
			if(colType.equals("java.lang.double") || colType.equals("java.lang.Integer") || colType.equals("java.lang.String") || colType.equals("java.lang.Date"))
				flagType = true;
			else
				flagType = false;
			if(!flagType)
				throw new DBAppException("Invalid Col Type");
		}
		if(!flagClust)
			throw new DBAppException("Invalid Clusterkey");
		writeToCSV(htblColNameType,htblColNameMin,htblColNameMax,indexNameHash, indexTypeHash,strClusteringKeyColumn, strTableName);
		table = new Table(strTableName);
		Table.saveTable(table,strTableName);
	}
	public void insertIntoTable(String strTableName,
			Hashtable htblColNameValue) throws DBAppException, CsvValidationException, IOException{
		File file = new File("src/Resourses/"+strTableName);
		if(!file.exists())
			throw new DBAppException("Table Does Not Exists");
		Enumeration<String> keys = htblColNameValue.keys();
		ArrayList <String> ColName = readKeyName(strTableName,addrCSV);
		while(keys.hasMoreElements()) {
			Boolean flag = false;
			String colName = keys.nextElement();
			for(int i = 0; i < ColName.size();i++)
				if(ColName.get(i).equals(colName))
					flag = true;
			if(!flag)
				throw new DBAppException("Invalid Col Name");
		}
		Table t = Table.loadTable(strTableName);
		String clustKey = DBApp.readClusterKeyName(strTableName, addrCSV);
		if (t.tablePages.size() == 0) {
			this.Page = new page("0");
			page.savePage(this.Page,"0",strTableName);
			t.tablePages.add("0");
			Table.saveTable(t, strTableName);
			page.insertIntoPage(htblColNameValue, "0",strTableName,addrCSV,clustKey);
			if (t.hasIndex) {
				for (int i = 0; i < t.tableIndex.size();i++) {
					Index index = Index.loadIndex(t.tableIndex.get(i));
					Object x = tupleReference.getX(htblColNameValue, index.colNames);
					Object y = tupleReference.getY(htblColNameValue, index.colNames);
					Object z = tupleReference.getZ(htblColNameValue, index.colNames);
					tupleReference tr = new tupleReference(x,y,z,"0");
					Index.insert(t.tableIndex.get(i), tr);
				}
			}
		}
		else {
			if(duplicateChecker(strTableName,htblColNameValue))
				throw new DBAppException("Duplicate clustering key");
			for(int i = 0; i<t.tablePages.size();i++) {
				String addrP1 = t.tablePages.get(i);
				page P1 = page.loadPage(addrP1,strTableName);
				keyComparator k1 = new keyComparator(strTableName,addrCSV,clustKey);
				if(P1.size() == 1 || P1.size() == 0) {
					page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
					if (t.hasIndex) {
						for (int j = 0; j < t.tableIndex.size();j++) {
							Index index = Index.loadIndex(t.tableIndex.get(j));
							Object x = tupleReference.getX(htblColNameValue, index.colNames);
							Object y = tupleReference.getY(htblColNameValue, index.colNames);
							Object z = tupleReference.getZ(htblColNameValue, index.colNames);
							tupleReference tr = new tupleReference(x,y,z,addrP1);
							Index.insert(t.tableIndex.get(i), tr);
						}
					}
					return;
				}
				else if(k1.compare(page.min(addrP1,strTableName),htblColNameValue)<=0 && k1.compare(page.max(addrP1,strTableName),htblColNameValue)>=0) {
					if(P1.size() < page.rows()) {
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						return;
					}
					else {
						Hashtable<String,Object> last = (Hashtable<String, Object>) P1.lastElement();
						page.deleteFromPage(last, addrP1,strTableName);
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						insertIntoTable(strTableName,last);
						return;
					}
				}
				else if((k1.compare(page.min(addrP1,strTableName),htblColNameValue)<=0 && k1.compare(page.max(addrP1,strTableName),htblColNameValue)<=0)){
					if(P1.size() < page.rows() && t.tablePages.get(t.tablePages.size()-1) == addrP1) {
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						return;
					}
					else if(P1.size() < page.rows() && k1.compare(page.max(t.tablePages.get(i+1),strTableName),htblColNameValue)>=0) {
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						return;
					}
					else if(t.tablePages.get(t.tablePages.size()-1) == addrP1) {
						int Pnum = Integer.parseInt(addrP1);
						Pnum++;
						this.Page = new page(Pnum+"");
						Page.savePage(this.Page,Pnum+"",strTableName);
						t.tablePages.add(Pnum+"");
						Table.saveTable(t, strTableName);
						page.insertIntoPage(htblColNameValue, Pnum+"",strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,Pnum+"");
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						return;
					}
				}
				else if((k1.compare(page.min(addrP1,strTableName),htblColNameValue)>=0 && k1.compare(page.max(addrP1,strTableName),htblColNameValue)>=0)){
					if(P1.size() < page.rows() && t.tablePages.get(0) == addrP1) {
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						return;
					}
					else {
						Hashtable<String,Object> last = (Hashtable<String, Object>) P1.lastElement();
						page.deleteFromPage(last, addrP1,strTableName);
						page.insertIntoPage(htblColNameValue, addrP1,strTableName,addrCSV,clustKey);
						if (t.hasIndex) {
							for (int j = 0; j < t.tableIndex.size();j++) {
								Index index = Index.loadIndex(t.tableIndex.get(j));
								Object x = tupleReference.getX(htblColNameValue, index.colNames);
								Object y = tupleReference.getY(htblColNameValue, index.colNames);
								Object z = tupleReference.getZ(htblColNameValue, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(t.tableIndex.get(i), tr);
							}
						}
						insertIntoTable(strTableName,last);
						return;
					}
				}
			}
		}
	}
	public void updateTable(String strTableName,
			String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue )
			throws DBAppException, CsvValidationException, IOException{
		File file = new File("src/Resourses/"+strTableName);
		if(!file.exists())
			throw new DBAppException("Table Does Not Exists");
		Enumeration<String> keys = htblColNameValue.keys();
		ArrayList <String> ColNam = readKeyName(strTableName,addrCSV);
		while(keys.hasMoreElements()) {
			Boolean flag = false;
			String colName = keys.nextElement();
			for(int i = 0; i < ColNam.size();i++)
				if(ColNam.get(i).equals(colName))
					flag = true;
			if(!flag)
				throw new DBAppException("Invalid Col Name");
		}
		String clustKeyType = readClusterKeyType(strTableName,addrCSV);
		Object clustKeyValue = null;
		switch(clustKeyType) {
			case "java.lang.double":
				clustKeyValue = Double.parseDouble(strClusteringKeyValue);
				break;
			case "java.lang.Integer":
				clustKeyValue = Integer.parseInt(strClusteringKeyValue);
				break;
			case "java.lang.String":
				clustKeyValue = (String)strClusteringKeyValue;
				break;
			case "java.lang.Date":
				clustKeyValue = java.util.Date.parse(clustKeyType);
				break;
		}
		Table table = Table.loadTable(strTableName);
		for(int i = 0; i < table.tablePages.size();i++) {
			String addrP1 = table.tablePages.get(i);
			page P1 = page.loadPage(addrP1,strTableName);
			String clustKey = DBApp.readClusterKeyName(strTableName, addrCSV);
			if(DBApp.compareKeys(strTableName,addrCSV,page.min(addrP1,strTableName).get(clustKey),clustKeyValue)<=0 && 
					DBApp.compareKeys(strTableName,addrCSV,page.max(addrP1,strTableName).get(clustKey),clustKeyValue)>=0) {
				for(int j = 0; j < P1.size();j++) {
					Hashtable<String,Object> key = (Hashtable<String, Object>) P1.get(j);
					if(key.get(clustKey).equals(clustKeyValue)) {
						if (table.hasIndex) {
							for (int k = 0; k<table.tableIndex.size();k++) {
								Index index = Index.loadIndex(table.tableIndex.get(k));
								Object x = tupleReference.getX(key, index.colNames);
								Object y = tupleReference.getY(key, index.colNames);
								Object z = tupleReference.getZ(key, index.colNames);
								tupleReference tr = new tupleReference(x,y,z);
								Index.delete(table.tableIndex.get(k), tr);
							}
						}
						Enumeration<String>colName = htblColNameValue.keys();
						while(colName.hasMoreElements()) {
							String ColName = colName.nextElement();
							key.replace(ColName, htblColNameValue.get(ColName));
						}
						if (table.hasIndex) {
							for (int k = 0; k<table.tableIndex.size();k++) {
								Index index = Index.loadIndex(table.tableIndex.get(k));
								Object x = tupleReference.getX(key, index.colNames);
								Object y = tupleReference.getY(key, index.colNames);
								Object z = tupleReference.getZ(key, index.colNames);
								tupleReference tr = new tupleReference(x,y,z,addrP1);
								Index.insert(table.tableIndex.get(k), tr);
							}
						}
						page.savePage(P1, addrP1,strTableName);
						return;
					}
				}
			}
		}
	}
	public void deleteFromTable(String strTableName,
			Hashtable<String,Object> htblColNameValue) throws DBAppException, CsvValidationException, IOException{
		File file = new File("src/Resourses/"+strTableName);
		if(!file.exists())
			throw new DBAppException("Table Does Not Exists");
		Enumeration<String> keys = htblColNameValue.keys();
		ArrayList <String> ColNam = readKeyName(strTableName,addrCSV);
		while(keys.hasMoreElements()) {
			Boolean flag = false;
			String colName = keys.nextElement();
			for(int i = 0; i < ColNam.size();i++)
				if(ColNam.get(i).equals(colName))
					flag = true;
			if(!flag)
				throw new DBAppException("Invalid Col Name");
		}
		Table table = Table.loadTable(strTableName);
		if (table.hasIndex) {
			for (int i = 0; i<table.tableIndex.size();i++) {
				Index index = Index.loadIndex(table.tableIndex.get(i));
				Object x = tupleReference.getX(htblColNameValue, index.colNames);
				Object y = tupleReference.getY(htblColNameValue, index.colNames);
				Object z = tupleReference.getZ(htblColNameValue, index.colNames);
				tupleReference tr = new tupleReference(x,y,z);
				String addrPage = Index.search(table.tableIndex.get(i), tr);
				System.out.println("Page: "+addrPage);
				Index.delete(table.tableIndex.get(i), tr);
				page.deleteFromPage(htblColNameValue, addrPage, strTableName);
				return;
			}
		}
		else {
			for(int i = 0; i < table.tablePages.size();i++) {
				String addrP1 = table.tablePages.get(i);
				page.deleteFromPage(htblColNameValue, addrP1,strTableName);
			}
		}
		for(int i = 0;i < table.tablePages.size();i++) {
			String addrP1 = table.tablePages.get(i);
			page P1 = page.loadPage(addrP1, strTableName);
			if(P1.size() == 0) {
				page.deletePage(addrP1,strTableName);
				Table.deletePage(addrP1, strTableName);
			}
		}
	}
	public void createIndex(String strTableName,
			String[] strarrColName) throws DBAppException, IOException, ParseException, CsvException{
		File file = new File("src/Resourses/"+strTableName);
		if(!file.exists())
			throw new DBAppException("Table Does Not Exists");
		boolean colFlag = false;
		if (strarrColName.length != 3) {
			throw new DBAppException("Number of col must only be 3");
		}
		for (int i = 0; i < strarrColName.length;i++) {
			try {
				if (strarrColName[i].equals(null)) {
					throw new DBAppException("Minimum number of col not met");
				}
			} catch (Exception e) {
				throw new DBAppException("Minimum number of col not met");
			}
		}
		for (int i = 0; i < this.colIndex.size(); i++) {
			if(strarrColName[0].equals(this.colIndex.get(i)) || strarrColName[1].equals(this.colIndex.get(i)) || strarrColName[2].equals(this.colIndex.get(i)))
				colFlag = true;
		}
		if (colFlag)
			throw new DBAppException("Col already has index");
		colFlag = false;
		ArrayList <String> ColNam = readKeyName(strTableName,addrCSV);
		for (int i = 0; i < ColNam.size();i++) {
			if (strarrColName[0].equals(ColNam.get(i)) || strarrColName[1].equals(ColNam.get(i)) || strarrColName[2].equals(ColNam.get(i)))
				colFlag = true;
			else {
				colFlag = false;
				break;
			}
		}
		if (!colFlag)
			throw new DBAppException("Col dose not exist in table");
		this.colIndex.add(strarrColName[0]);
		this.colIndex.add(strarrColName[1]);
		this.colIndex.add(strarrColName[2]);
		Object minX = rangeX(strTableName, strarrColName).get(0);
		Object maxX = rangeX(strTableName, strarrColName).get(1);
		Object minY = rangeY(strTableName, strarrColName).get(0);
		Object maxY = rangeY(strTableName, strarrColName).get(1);
		Object minZ = rangeZ(strTableName, strarrColName).get(0);
		Object maxZ = rangeZ(strTableName, strarrColName).get(1);
		Octree octree = new Octree(minX,maxX,minY,maxY,minZ,maxZ,strTableName,strarrColName);
		Index index = new Index(octree,strarrColName);
		String addrIndex = strTableName+strarrColName[0]+strarrColName[1]+strarrColName[2];
		Index.saveIndex(index, addrIndex);
		Table t = Table.loadTable(strTableName);
		Table.addIndex(addrIndex, strTableName);
		//updateCSV(strTableName, strarrColName, addrIndex);
		if (t.tablePages.size() != 0) {
			for(int i = 0; i < t.tablePages.size();i++) {
				page p = page.loadPage(t.tablePages.get(i), strTableName);
				for (int j = 0; j < p.size(); j++) {
					Object x = tupleReference.getX((Hashtable<String, Object>) p.get(j), strarrColName);
					Object y = tupleReference.getY((Hashtable<String, Object>) p.get(j), strarrColName);
					Object z = tupleReference.getZ((Hashtable<String, Object>) p.get(j), strarrColName);
					tupleReference tr = new tupleReference(x,y,z,t.tablePages.get(i));
					Index.insert(addrIndex,tr);
				}
			}
		}
	}
	public ArrayList<Object> rangeX(String addrTable, String [] strarrColName) throws CsvValidationException, IOException, ParseException {
		String keyType = DBApp.readKeyType(addrTable, addrCSV, strarrColName[0]);
		Object minX = null;
		Object maxX = null;
		ArrayList<Object> result = new ArrayList<>();
		switch(keyType) {
			case "java.lang.double":
				minX = Double.parseDouble(DBApp.readMin(addrTable, addrCSV, strarrColName[0]));
				maxX = Double.parseDouble(DBApp.readMax(addrTable, addrCSV, strarrColName[0]));
				break;
			case "java.lang.Integer":
				minX = Integer.parseInt(DBApp.readMin(addrTable, addrCSV, strarrColName[0]));
				maxX = Integer.parseInt(DBApp.readMax(addrTable, addrCSV, strarrColName[0]));
				break;
			case "java.lang.String":
				minX = DBApp.readMin(addrTable, addrCSV, strarrColName[0]);
				maxX = DBApp.readMax(addrTable, addrCSV, strarrColName[0]);
				break;
			case "java.lang.Date":
				minX = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMin(addrTable, addrCSV, strarrColName[0]));
				maxX = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMax(addrTable, addrCSV, strarrColName[0]));
				break;
		}
		result.add(minX);
		result.add(maxX);
		return result;
	}
	public ArrayList<Object> rangeY(String addrTable, String [] strarrColName) throws CsvValidationException, IOException, ParseException {
		String keyType = DBApp.readKeyType(addrTable, addrCSV, strarrColName[1]);
		Object minY = null;
		Object maxY = null;
		ArrayList<Object> result = new ArrayList<>();
		switch(keyType) {
			case "java.lang.double":
				minY = Double.parseDouble(DBApp.readMin(addrTable, addrCSV, strarrColName[1]));
				maxY = Double.parseDouble(DBApp.readMax(addrTable, addrCSV, strarrColName[1]));
				break;
			case "java.lang.Integer":
				minY = Integer.parseInt(DBApp.readMin(addrTable, addrCSV, strarrColName[1]));
				maxY = Integer.parseInt(DBApp.readMax(addrTable, addrCSV, strarrColName[1]));
				break;
			case "java.lang.String":
				minY = DBApp.readMin(addrTable, addrCSV, strarrColName[1]);
				maxY = DBApp.readMax(addrTable, addrCSV, strarrColName[1]);
				break;
			case "java.lang.Date":
				minY = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMin(addrTable, addrCSV, strarrColName[1]));
				maxY = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMax(addrTable, addrCSV, strarrColName[1]));
				break;
		}
		result.add(minY);
		result.add(maxY);
		return result;
	}
	public ArrayList<Object> rangeZ(String addrTable, String [] strarrColName) throws CsvValidationException, IOException, ParseException {
		String keyType = DBApp.readKeyType(addrTable, addrCSV, strarrColName[2]);
		Object minZ = null;
		Object maxZ = null;
		ArrayList<Object> result = new ArrayList<>();
		switch(keyType) {
			case "java.lang.double":
				minZ = Double.parseDouble(DBApp.readMin(addrTable, addrCSV, strarrColName[2]));
				maxZ = Double.parseDouble(DBApp.readMax(addrTable, addrCSV, strarrColName[2]));
				break;
			case "java.lang.Integer":
				minZ = Integer.parseInt(DBApp.readMin(addrTable, addrCSV, strarrColName[2]));
				maxZ = Integer.parseInt(DBApp.readMax(addrTable, addrCSV, strarrColName[2]));
				break;
			case "java.lang.String":
				minZ = DBApp.readMin(addrTable, addrCSV, strarrColName[2]);
				maxZ = DBApp.readMax(addrTable, addrCSV, strarrColName[2]);
				break;
			case "java.lang.Date":
				minZ = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMin(addrTable, addrCSV, strarrColName[2]));
				maxZ = new SimpleDateFormat("dd/MM/yyyy").parse(DBApp.readMax(addrTable, addrCSV, strarrColName[2]));
				break;
		}
		result.add(minZ);
		result.add(maxZ);
		return result;
	}
	public static ArrayList<String> extractHash(Hashtable<String,String> hashTable, String tableName) {
		 Enumeration<String> keys = hashTable.keys();
		 ArrayList<String> keysCSV = new ArrayList<String>();
		 while (keys.hasMoreElements()) {
	            String key = keys.nextElement();
	            keysCSV.add(key + ":" + tableName);
	            colType.add(hashTable.get(key));
	        }
		return keysCSV;
	}
	private static void writeToCSV(Hashtable<String, String> hashCol, Hashtable<String, String> hashMin, Hashtable<String, String> hashMax, Hashtable<String, String> indexHash, Hashtable<String, String> indtypeHash, String clstr, String tbleName)
	   {
		    boolean append = true;

		    try {
		        FileWriter csvWriter = new FileWriter(addrCSV, append);
		        Enumeration<String> colKeys = hashCol.keys();
		        Enumeration<String> minKeys = hashMin.keys();
		        Enumeration<String> maxKeys = hashMax.keys();

		        while(colKeys.hasMoreElements()) {
		               String colName = colKeys.nextElement();
		               String min = minKeys.nextElement();
		               String max = maxKeys.nextElement();
		               String colType = hashCol.get(colName);
		               csvWriter.append(tbleName);
		               csvWriter.append(CSV_SEPARATOR);
		               csvWriter.append(colName);
		               csvWriter.append(CSV_SEPARATOR);
		               csvWriter.append(colType);
		               csvWriter.append(CSV_SEPARATOR);
		               if(colName.equals(clstr)) {
		            	   csvWriter.append("True");
		               }
		               else {
		            	   csvWriter.append("False");
		               }
		               csvWriter.append(CSV_SEPARATOR);
		               if(indexHash.isEmpty())
		            	   csvWriter.append("null");
		               csvWriter.append(CSV_SEPARATOR);
		               if(indtypeHash.isEmpty())
		            	   csvWriter.append("null");
		               csvWriter.append(CSV_SEPARATOR);
		               csvWriter.append(hashMin.get(min));
		               csvWriter.append(CSV_SEPARATOR);
		               csvWriter.append(hashMax.get(min));
		               csvWriter.append("\n");

		        }
		        csvWriter.close();

		    } catch (IOException e) {
		        e.printStackTrace();
		    }
	   }
	public Boolean duplicateChecker(String strTableName,Hashtable htblColNameValue) throws CsvValidationException, IOException {
		Table t = Table.loadTable(strTableName);
		String clustKey = DBApp.readClusterKeyName(strTableName, addrCSV);
		Boolean flag = false;
		for(int i = 0; i < t.tablePages.size();i++) {
			page P1 = page.loadPage(t.tablePages.get(i), strTableName);
			for(int j = 0;j < P1.size();j++) {
			Hashtable<String,Object> tuple = (Hashtable<String, Object>) P1.get(j);
			if(tuple.get(clustKey).equals(htblColNameValue.get(clustKey)))
				flag = true;
			}
		}
		return flag;
	}
	public static String readClusterKeyName(String strTableName, String addrCSV) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        String result = "";
	        while ((nextLine = reader.readNext()) != null) {
	        	if(nextLine[0].equals(strTableName) && nextLine[3].equals("True"))
	        		result = nextLine[1];
	        }
	        reader.close();
	        return result;
	    }
	public static String readKeyType(String strTableName, String addrCSV, String colName) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        String result = "";
	        while ((nextLine = reader.readNext()) != null) {
	        	if(nextLine[0].equals(strTableName) && nextLine[1].equals(colName))
	        		result = nextLine[2];
	        }
	        reader.close();
	        return result;
	    }
	public static String readMin(String strTableName, String addrCSV, String colName) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        String result = null;
	        while ((nextLine = reader.readNext()) != null) {
	        	if(nextLine[0].equals(strTableName) && nextLine[1].equals(colName))
	        		result = nextLine[6];
	        }
	        reader.close();
	        return result;
	    }
	public static String readMax(String strTableName, String addrCSV, String colName) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        String result = null;
	        while ((nextLine = reader.readNext()) != null) {
	        	if(nextLine[0].equals(strTableName) && nextLine[1].equals(colName))
	        		result = nextLine[7];
	        }
	        reader.close();
	        return result;
	    }
	public static ArrayList<String> readKeyName(String strTableName, String addrCSV) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        ArrayList <String> result = new ArrayList<>();
	        while ((nextLine = reader.readNext()) != null)
	        		result.add(nextLine[1]);
	        reader.close();
	        return result;
	    }
	public static String readClusterKeyType(String strTableName, String addrCSV) throws CsvValidationException, IOException {
	    	String csvFile = addrCSV;
	        CSVReader reader = new CSVReader(new FileReader(csvFile));
	        String[] nextLine;
	        String result = "";
	        while ((nextLine = reader.readNext()) != null) {
	           if(nextLine[0].equals(strTableName) && nextLine[3].equals("True"))
	        	   result = nextLine[2];
	        }
	        reader.close();
	        return result;
	    }
	public static int compareKeys(String strTableName, String addrCSV, Object O1, Object O2) throws CsvValidationException, IOException {
	    	String keyType = DBApp.readClusterKeyType(strTableName, addrCSV);
	    	switch (keyType) {
	    		case "java.lang.double":
	    			return ((Double) O1).compareTo((Double) O2);
	    		case "java.lang.Integer":
	    			return ((Integer) O1).compareTo((Integer) O2);
	    		case "java.lang.String":
	    			return ((String) O1).compareTo((String) O2);
	    		case "java.lang.Date":
	    			return ((Date) O1).compareTo((Date) O2);
	    	}
	    	return 0;
	    }
	public static int compareKeys(String strTableName, String addrCSV, String colName, Object O1, Object O2) throws CsvValidationException, IOException {
	    	String keyType = DBApp.readKeyType(strTableName, addrCSV, colName);
	    	switch(keyType) {
	    		case "java.lang.double":
	    			return ((Double) O1).compareTo((Double) O2);
	    		case "java.lang.Integer":
	    			return ((Integer) O1).compareTo((Integer) O2);
	    		case "java.lang.String":
	    			return ((String) O1).compareTo((String) O2);
	    		case "java.lang.Date":
	    			return ((Date) O1).compareTo((Date) O2);
	    	}
	    	return 0;
	    }
	public static void addEntry(String strTableName, Hashtable<String,String> hash) {
	    	ArrayList<String> content = new ArrayList<String>();
	    	ArrayList<String> keysCSV = extractHash(hash, strTableName);
	    	for(int i=0; i<keysCSV.size();i++) {
	    		
	    		String s= keysCSV.get(i);
	    		
	            String[] parts = s.split(":");
	            String tableName = parts[1];
	            content.add(parts[0]);
	    		entries.add(new entry(strTableName, colType.get(i), 0,100000, content.get(i)));
	    	}
	    }
	public static void checkCluster(String clstr, String strTableName,	Hashtable<String,String> hash){
			ArrayList<String> content = new ArrayList<String>();
	    	ArrayList<String> keysCSV = extractHash(hash, strTableName);
			for(int i=0;i<keysCSV.size();i++) {
				clstrArray.add("False");
			}
	
			for(int i=0;i<keysCSV.size();i++) {
	            String[] parts = keysCSV.get(i).split(":");
	            String tableName = parts[1];
	            content.add(parts[0]);	            
				if(content.get(i).equals(clstr) && tableName.equals(tableName)) {
					clstrArray.set(i, "True");
				}
			}
		}
	public static void checkCluster(String clstr){
			for(int i=0;i<colKeys.size();i++) {
				clstrArray.add("False");
			}
			for(int i=0;i<colKeys.size();i++) {
				if(colKeys.get(i)==clstr) {
					clstrArray.set(i, "True");
				}
			}
		}
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException{
			SQLTerm[] sqlArray = arrSQLTerms;
			String operator = "";
			Iterator result; 
			ArrayList<Hashtable> resultSet = new ArrayList<Hashtable>();
			String tableName = sqlArray[0]._strTableName;
			Table currentTable = Table.loadTable(tableName);
			for(int j=0;j<currentTable.tablePages.size();j++) {
				page currentPage = page.loadPage(currentTable.tablePages.get(j),tableName);
					for(int k=0;k<currentPage.size();k++) {
						boolean flag = false;
						ArrayList<Boolean> flags = new ArrayList<Boolean>();
						for(int i=0;i<sqlArray.length;i++){
							operator = sqlArray[i]._strOperator;
							
								switch(operator) {
								case "<":
									if(sqlArray[i]._objValue instanceof Integer) {
										if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) < (int)(sqlArray[i]._objValue)) {
											flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) == -1) {
										flag = true;
					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) < (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) == -1) {
										flag = true;					        }
						
					}
					break;
				case ">":
				
								if(sqlArray[i]._objValue instanceof Integer) {
									if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) > (int)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) == 1) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) > (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) == 1) {
										flag = true;					        }
						
					}
					break;
				case "=":
					if(sqlArray[i]._objValue instanceof Integer) {
									if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) == (int)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) == 0) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) == (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) == 0) {
										flag = true;					        }
						
					}
					break;
				case ">=":
					if(sqlArray[i]._objValue instanceof Integer) {
									if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) >= (int)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) >= 0) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) >= (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) >=0) {
										flag = true;					        }
											}
					break;
				case "<=":
					if(sqlArray[i]._objValue instanceof Integer) {
									if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) <= (int)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) <= 0) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) <= (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) <=0) {
										flag = true;					        }
						
					}
					break;
				case "!=":
					if(sqlArray[i]._objValue instanceof Integer) {
									if((int)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) != (int)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof String) {
									if(((String)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((String)(sqlArray[i]._objValue)) !=0) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Double) {
									if((Double)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName) != (Double)(sqlArray[i]._objValue)) {
										flag = true;					        }
						}
								else if(sqlArray[i]._objValue instanceof Date) {
									if(((Date)((Hashtable<String,Object>)currentPage.get(k)).get(sqlArray[i]._strColumnName)).compareTo((Date)(sqlArray[i]._objValue)) !=0) {
										flag = true;
					        }
						
					}
					break;
				default: throw new  DBAppException("Invalid input");
					
			}
								flags.add(flag);
			}
			for(int m = 0; m<strarrOperators.length; m++) {
				switch(strarrOperators[m]) {
				case "AND":
					flags.set(0, flags.get(0) && flags.get(1));
					flags.remove(1);
					break;
				case "OR":
					flags.set(0, flags.get(0) || flags.get(1));
					flags.remove(1);
					break;
				case "XOR":
					flags.set(0, flags.get(0) && flags.get(1));
					flags.remove(1);
					break;
				}
			}
			
		
		if (flags.get(0)) {
			resultSet.add((Hashtable<String,Object>)currentPage.get(k));
		}
		}
			}
			
			 result = resultSet.iterator();
				return result;
	}
	public void updateCSV(String tableName, String [] colNames, String indexName) throws IOException, CsvException {
	       int columnToOverwrite = 4; // 0-indexed column number
	       int columnToOverwrite2 = 5; // 0-indexed column number
	        try (CSVReader reader = new CSVReader(new FileReader(addrCSV))) {
	            String[] header = reader.readNext(); // Read the header row
	            String[] line;
	            while ((line = reader.readNext()) != null) {
	            	for(int i=0;i<colNames.length;i++) {
	                if (line[1].equals(colNames[i]) && line[0].equals(tableName)) {
	                    line[columnToOverwrite] = indexName;
	                    line[columnToOverwrite2] = "OctTree";
	                }
	            	}
	            }

	            // Write the updated data back to the CSV file
	            try (CSVWriter writer = new CSVWriter(new FileWriter(addrCSV))) {
	                writer.writeNext(header);
	                writer.writeAll(reader.readAll());
	            }
	        }
	    }
	private static void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


//            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
//            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
//            int day = Integer.parseInt(fields[0].trim().substring(8));
//
//            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", fields[0].trim());

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

 private static void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

//            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
//            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
//            int day = Integer.parseInt(fields[3].trim().substring(8));
//
//            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", fields[3].trim());

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
 private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
           // int year = Integer.parseInt(date.substring(0, 4));
           // int month = Integer.parseInt(date.substring(5, 7));
           // int day = Integer.parseInt(date.substring(8));

           // Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", date);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
 private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
 private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.lang.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.lang.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }
	
	public static void main(String[]args) throws Exception {
//		 Hashtable<String, Object> hashtable = new Hashtable<>();
//		 Hashtable<String, Object> hashtable2 = new Hashtable<>();
//		 hashtable.put("3", "5");
//		 hashtable.put("4", "3");
//		 hashtable2.put("3", "5");
//		 hashtable2.put("4", "3");
//		 Properties prop = new Properties();
//		
		DBApp db = new DBApp();
		 db.init();
//		 Table t = Table.loadTable("transcripts");
//		 for(int i = 0; i < t.tablePages.size();i++) {
//			 page p = page.loadPage(t.tablePages.get(i), "transcripts");
//			 for(int j = 0; j < p.size();j++) {
//				 System.out.println("Record "+ p.get(j));
//			 }
//			 System.out.println("----------------------------------");
//		 }
		 //createCoursesTable(db);
		// createPCsTable(db);
		 //createTranscriptsTable(db);
		//createStudentTable(db);
		// insertPCsRecords(db,500);
		// insertTranscriptsRecords(db,500);
		//insertStudentRecords(db,500);
//		 insertCoursesRecords(db,500);
//		 System.out.println("done");
		//Table t= readTable("students");
		//printTable(t);
		 //Record {course_id=1943, course_name=rodOEe, hours=11, date_added=1908-10-29}
//		 Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("hours",16);
		//htblColNameValue.put("last_name", "Hamadaa");
		//htblColNameValue.put("ghalat",new Date(1991,12,26));
		 //db.insertIntoTable( "pcs" , htblColNameValue );
		// db.updateTable("students", "44-1562", htblColNameValue);
		//db.deleteFromTable("courses", htblColNameValue);
//		 db.createIndex("students", "first_name");
//		SQLTerm[] terms = new SQLTerm[2];
//		terms[0] = new SQLTerm("students", "id", "=", "82-8772");
//		terms[1] = new SQLTerm("students", "gpa", ">", "0.85");
//		String[] strarrOperators = { "AND" };
//		Iterator<String> res = db.selectFromTable(terms, strarrOperators);
//		while(res.hasNext()) {
//			String s = (String) res.next();
//			System.out.println(s);
//			
//		}
		 
//		 SQLTerm[] arrSQLTerms;
//       arrSQLTerms = new SQLTerm[2];
//      arrSQLTerms[0]= new SQLTerm();
//      arrSQLTerms[1]=new SQLTerm();
////      arrSQLTerms[2]=new SQLTerm();
//       arrSQLTerms[0]._strTableName = "courses";
//       arrSQLTerms[0]._strColumnName= "course_id";
//       arrSQLTerms[0]._strOperator = "=";
//       arrSQLTerms[0]._objValue = "0950";
//       arrSQLTerms[1]._strTableName = "courses";
//       arrSQLTerms[1]._strColumnName= "hours";
//       arrSQLTerms[1]._strOperator = "!=";
//       arrSQLTerms[1]._objValue =16;
//       arrSQLTerms[2]._strTableName = "courses";
//       arrSQLTerms[2]._strColumnName= "course_name";
//       arrSQLTerms[2]._strOperator = "=";
//       arrSQLTerms[2]._objValue = "FjGZmL";
//       String[]strarrOperators = new String[1];
//       strarrOperators[0] = "OR";
//       strarrOperators[1] = "OR";

//   long start = System.currentTimeMillis();
////
////  
////
//       Iterator it = db.selectFromTable(arrSQLTerms, strarrOperators);
//
//       while(it.hasNext()) {
//               System.out.println(it.next());
//       }
//       long end = System.currentTimeMillis();
//       
//       System.out.println("time difference is "+(end-start));
		 //db.createIndex("students", new String[] {"id","first_name","last_name"});
	}
}
