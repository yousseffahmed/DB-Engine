import java.io.Serializable;

public class entry implements Serializable{
	
	String tableName;
	String hashKey;
	String colType;
	int max;
	int min;
	public entry(String tableName, String colType, int max, int min, String key) {
		super();
		this.tableName = tableName;
		this.colType = colType;
		this.max = max;
		this.min = min;
		this.hashKey = key;
	}
	public String getTableName() {
		return tableName;
	}
	public String gethashKey() {
		return hashKey;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColType() {
		return colType;
	}
	public void setColType(String colType) {
		this.colType = colType;
	}
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
	}
	public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	
	

}
