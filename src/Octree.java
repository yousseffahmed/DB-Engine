import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import javax.naming.directory.SearchControls;

import com.opencsv.exceptions.CsvValidationException;

public class Octree implements Serializable{
	String addrTable;
	String addrCSV = "metadata.csv";
	String [] strarrColName;
	Node root;
	public Octree (Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ, String addrTable, String [] strarrColName) {
		this.addrTable = addrTable;
		this.strarrColName = strarrColName;
		this.root = new Node(minX,maxX,minY,maxY,minZ,maxZ);
	}
	public void insert (tupleReference tr) throws CsvValidationException, IOException {
		insHelper(tr,this.root);
	}
	public void insHelper (tupleReference tr, Node node) throws CsvValidationException, IOException {
		String typeX = DBApp.readKeyType(this.addrTable, this.addrCSV, this.strarrColName[0]);
		String typeY = DBApp.readKeyType(this.addrTable, this.addrCSV, this.strarrColName[1]);
		String typeZ = DBApp.readKeyType(this.addrTable, this.addrCSV, this.strarrColName[2]);
		ArrayList<tupleReference> records = new ArrayList<>();
		if (node.isLeaf()) {
			if (node.nodeContent.size() < Node.max())
				node.insert(tr);
			else {
			node.createChildren(node.minX, node.maxX, node.minY, node.maxY, node.minZ, node.maxZ, typeX, typeY, typeZ,node);
			for (int i = 0; i < node.nodeContent.size(); i++)
				records.add(node.nodeContent.get(i));
			node.nodeContent.clear();
			records.add(tr);
			for (int i = 0; i < records.size(); i++)
				insHelper(records.get(i),node);
			}
		}
		else {
			for (int i = 0; i < node.children.size();i++) {
				Node currNode = node.children.get(i);
				if(currNode.correctPos(tr.x, tr.y, tr.z, this.addrTable, this.addrCSV, this.strarrColName)) {
					insHelper(tr,currNode);
					break;
				}
			}
		}
	}
	public void delete (tupleReference tr) throws CsvValidationException, IOException {
		deleteHelper(tr,this.root);
	}
	public void deleteHelper (tupleReference tr, Node node) throws CsvValidationException, IOException {
		if (node.isLeaf()) {
			if (node.correctPos(tr.x, tr.y, tr.z, this.addrTable, this.addrCSV, this.strarrColName)) {
				for (int i = 0; i < node.nodeContent.size();i++) {
					if (DBApp.compareKeys(addrTable, addrCSV, strarrColName[0], node.nodeContent.get(i).x, tr.x) == 0
							&& DBApp.compareKeys(addrTable, addrCSV, strarrColName[1], node.nodeContent.get(i).y, tr.y) == 0
							&& DBApp.compareKeys(addrTable, addrCSV, strarrColName[2], node.nodeContent.get(i).z, tr.z) == 0) {
						 node.nodeContent.remove(i);
						break;
					}
				}
			}
		}
		else {
			for (int i = 0; i < node.children.size(); i++) {
				Node currNode = node.children.get(i);
				deleteHelper(tr, currNode);
			}
		}
	}
	public String search(tupleReference tr) throws CsvValidationException, IOException {
	    return searchHelper(tr,this.root);
	}

	public String searchHelper(tupleReference tr, Node node) throws CsvValidationException, IOException {
	    String result = "";
	    if (node.isLeaf()) {
	        if (node.correctPos(tr.x, tr.y, tr.z, this.addrTable, this.addrCSV, this.strarrColName)) {
	            for (int i = 0; i < node.nodeContent.size();i++) {
	                if (DBApp.compareKeys(addrTable, addrCSV, strarrColName[0], node.nodeContent.get(i).x, tr.x) == 0
	                        && DBApp.compareKeys(addrTable, addrCSV, strarrColName[1], node.nodeContent.get(i).y, tr.y) == 0
	                        && DBApp.compareKeys(addrTable, addrCSV, strarrColName[2], node.nodeContent.get(i).z, tr.z) == 0) {
	                    result = node.nodeContent.get(i).addrPage;
	                    return result;
	                }
	            }
	        }
	    }
	    else {
	        for (int i = 0; i < node.children.size(); i++) {
	            Node currNode = node.children.get(i);
	            result = searchHelper(tr, currNode);
	            if (!result.equals("")) {
	                return result;
	            }
	        }
	    }
	    return result;
	}

	public void printOctree() {
        printNode(root, "");
    }

    private void printNode(Node node, String indent) {
        if (node == null) {
            System.out.println(indent + "null");
            return;
        }

        System.out.println(indent + "Node");
        System.out.println(indent + "Min X: " + node.minX);
        System.out.println(indent + "Max X: " + node.maxX);
        System.out.println(indent + "Min Y: " + node.minY);
        System.out.println(indent + "Max Y: " + node.maxY);
        System.out.println(indent + "Min Z: " + node.minZ);
        System.out.println(indent + "Max Z: " + node.maxZ);

        if (node.isLeaf()) {
            System.out.println(indent + "Content: ");
            for (int i = 0; i < node.nodeContent.size(); i++) {
                System.out.println(node.nodeContent.get(i));
            }
        } else {
            for (Node child : node.children) {
                printNode(child,indent + "  ");
            }
        }
    }
	public ArrayList<Node> searchSelect(Object x, Object y, Object z, String[] op, Node n) {
		Node currNode = n;
		Boolean flag = false;
		ArrayList<Boolean> flags = new ArrayList<Boolean>();
		ArrayList<Node> nodes = new ArrayList<Node>();
		if (!currNode.isLeaf()) {
			for(int i= 0; i<currNode.children.size();i++) {
			switch(op[0]) {
			case "=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0 && ((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
						flag = true;
						flags.add(flag);

				}
				break;
			case ">":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<0) {
					flag = true;
					flags.add(flag);

			}
				break;
			case "<":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>0) {
					flag = true;
					flags.add(flag);

			}
				break;
			case ">=":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
					flag = true;
					flags.add(flag);

			}
				break;
			case "<=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0) {
					flag = true;
					flags.add(flag);

			}
				break;
			}
			flag = false;
			switch(op[1]) {
			case "=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0 && ((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
						flag = true;
						flags.add(flag);
				}
				break;
			case ">":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case "<":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case ">=":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case "<=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0) {
					flag = true;
					flags.add(flag);
			}
				break;
			}
			flag = false;
			switch(op[2]) {
			case "=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0 && ((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
						flag = true;
						flags.add(flag);
				}
				break;
			case ">":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case "<":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case ">=":
				if(((Comparable) currNode.children.get(i).maxX).compareTo(x)<=0) {
					flag = true;
					flags.add(flag);
			}
				break;
			case "<=":
				if(((Comparable) currNode.children.get(i).minX).compareTo(x)>=0) {
					flag = true;
					flags.add(flag);
			}
				break;
			}
			if(flags.get(0) && flags.get(1) && flags.get(2)) {
				nodes.add(currNode.children.get(i));
				searchSelect(x, y, z, op, currNode.children.get(i));
			}
			
			}
		}
		return nodes;
		
			
			
			
		
	}
}