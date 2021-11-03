package ipp_bdd;

import java.util.ArrayList;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//System.out.println("s");
		 DataTable table= new DataTable();
//		 ArrayList<String> wantedcolumn= new ArrayList<String>();
//		 wantedcolumn.add("P_PARTKEY");
//		 wantedcolumn.add("P_NAME");
//		 wantedcolumn.add("P_MFGR");
//		 wantedcolumn.add("P_BRAND");
//		 wantedcolumn.add("P_TYPE");
//		 wantedcolumn.add("P_SIZE");
//		 wantedcolumn.add("P_CONTAINER");
//		 wantedcolumn.add("P_RETAILPRICE");
//		 wantedcolumn.add("P_COMMENT");
		 String path = "50MoColumns/";

		boolean type_buffer = true;
		ArrayList<String> type_columns = new ArrayList<String>();
		DataTable db = new DataTable();
		ColumnNameConstructor columnNameConstructor=new ColumnNameConstructor("Lineitem");
		db.load(path, columnNameConstructor.getcolmunName(), type_buffer, type_columns);
		db.print(20);
		System.out.println();
	}
}
	

