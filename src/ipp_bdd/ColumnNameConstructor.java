package ipp_bdd;

import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;

public class ColumnNameConstructor {
	private ArrayList<String> columnName;
	
	public ColumnNameConstructor(String tableString) {
		columnName=new ArrayList<String>();
		if(tableString.equals("Part")) {
			columnName.add("P_PARTKEY");
			columnName.add("P_NAME");
			columnName.add("P_MFGR");
			columnName.add("P_BRAND");
			columnName.add("P_TYPE");
			columnName.add("P_SIZE");
			columnName.add("P_CONTAINER");
			columnName.add("P_RETAILPRICE");
			columnName.add("P_COMMENT");
		}
		else if(tableString.equals("Supplier")) {
			columnName.add("S_SUPPKEY ");
			columnName.add("S_NAME");
			columnName.add("S_ADDRESS");
			columnName.add("S_NATIONKEY");
			columnName.add("S_PHONE");
			columnName.add("S_ACCTBAL");
			columnName.add("S_COMMENT");
		}
		else if(tableString.equals("Partsupp")) {
			columnName.add("PS_PARTKEY");
			columnName.add("PS_SUPPKEY");
			columnName.add("PS_AVAILQTY");
			columnName.add("PS_SUPPLYCOST");
			columnName.add("PS_COMMENT");
		}
		else if(tableString.equals("Customer")) {
			columnName.add("C_CUSTKEY");
			columnName.add("C_NAME");
			columnName.add("C_ADDRESS");
			columnName.add("C_NATIONKEY");
			columnName.add("C_PHONE");
			columnName.add("C_ACCTBAL");
			columnName.add("C_MKTSEGMENT");
			columnName.add("C_COMMENT");
				}
		else if(tableString.equals("Orders")) {
			columnName.add("O_ORDERKEY");
			columnName.add("O_CUSTKEY");
			columnName.add("O_ORDERSTATUS");
			columnName.add("O_TOTALPRICE");
			columnName.add("O_ORDERDATE");
			columnName.add("O_ORDERPRIORITY");
			columnName.add("O_CLERK");
			columnName.add("O_SHIPPRIORITY");
			columnName.add("O_COMMENT");
		}
		else if(tableString.equals("Lineitem")) {
			columnName.add("L_ORDERKEY");
			columnName.add("L_PARTKEY");
			columnName.add("L_SUPPKEY");
			columnName.add("L_LINENUMBER");
			columnName.add("L_QUANTITY");
			columnName.add("L_EXTENDEDPRICE");
			columnName.add("L_DISCOUNT");
			columnName.add("L_TAX");
			columnName.add("L_RETURNFLAG");
			columnName.add("L_LINESTATUS");
			columnName.add("L_SHIPDATE");
			columnName.add("L_COMMITDATE");
			columnName.add("L_RECEIPTDATE");
			columnName.add("L_SHIPINSTRUCT");
			columnName.add("L_SHIPMODE");
			columnName.add("L_COMMENT");
		}
		else if(tableString.equals("Nation")) {
			columnName.add("N_NATIONKEY");
			columnName.add("N_NAME");
			columnName.add("N_REGIONKEY");
			columnName.add("N_COMMENT");
		}
		else if(tableString.equals("Region")) {
			columnName.add("R_REGIONKEY");
			columnName.add("R_NAME");
			columnName.add("R_COMMENT");
		}
	}
	
	public ArrayList<String> getcolmunName(){
		return columnName;
	}
	
}
