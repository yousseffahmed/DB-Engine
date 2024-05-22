import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class tupleReference implements Serializable{
	Object x;
	Object y;
	Object z;
	String addrPage;
	public tupleReference(Object x2, Object y2, Object z2, String addrPage) {
		this.x = x2;
		this.y = y2;
		this.z = z2;
		this.addrPage = addrPage;
	}
	public tupleReference(Object x2, Object y2, Object z2) {
		this.x = x2;
		this.y = y2;
		this.z = z2;
	}
	public static Object getX(Hashtable<String,Object> tuple, String [] strarrColName) {
		return tuple.get(strarrColName[0]);
	}
	public static Object getY(Hashtable<String,Object> tuple, String [] strarrColName) {
		return tuple.get(strarrColName[1]);
	}
	public static Object getZ(Hashtable<String,Object> tuple, String [] strarrColName) {
		return tuple.get(strarrColName[2]);
	}
}
