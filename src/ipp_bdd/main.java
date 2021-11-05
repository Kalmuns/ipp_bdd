package ipp_bdd;

import java.lang.ref.Reference;
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

		DataTable db1= new DataTable();
		DataTable db2=new DataTable();
		ColumnNameConstructor columnNameConstructor1=new ColumnNameConstructor("Nation");
		ColumnNameConstructor columnNameConstructor2=new ColumnNameConstructor("Region");
		db1.load(path, columnNameConstructor1.getcolmunName(), type_buffer, type_columns);
		db2.load(path, columnNameConstructor2.getcolmunName(), type_buffer, type_columns);
		ArrayList<String> columnto_join1 = new ArrayList<String>();
		ArrayList<String> columnto_join2 = new ArrayList<String>();
		
		columnto_join1.add("N_REGIONKEY");
		columnto_join1.add("N_NAME");
		columnto_join2.add("R_REGIONKEY");
		columnto_join2.add("R_NAME");
		
		
		ArrayList<String> comparators =new ArrayList<String>();
		comparators.add("contain");
		ArrayList<ArrayList<Object>> tocompare=new ArrayList<ArrayList<Object>>();
		ArrayList<Object> buffertocompare= new ArrayList<Object> ();
		buffertocompare.add(new String("ir"));
		tocompare.add(buffertocompare);
		ArrayList<ArrayList<Object>> ref=new ArrayList<ArrayList<Object>>();
	
		DataTable db3;
		db3=db1.sortjoin(db2, columnto_join1, columnto_join2,"=" ,false);
		ref.add(new ArrayList<Object>(db3.get_column("N_NAME")));
		db3.filter(ref, comparators, tocompare);
		db3.print(2);
		
		//		db.print(25);
//		db.sort("N_REGIONKEY");
//		db.print(25);
		
		//////////////////////////////////////////////////////////////////

		DataTable db = new DataTable();
		ColumnNameConstructor columnNameConstructor=new ColumnNameConstructor("Nation");
		ArrayList<String> column_togroup = new ArrayList<String>(2);
		column_togroup.add("N_REGIONKEY");
		ArrayList<String> aggregation = new ArrayList<String>(2);
		aggregation.add("sum");
		aggregation.add("concat");
		aggregation.add("average");
		aggregation.add("concat");
		db.load(path, columnNameConstructor.getcolmunName(), type_buffer, type_columns);
		db.print(25);
		db.groupBy(column_togroup, aggregation);
		db.sort("N_REGIONKEY");
		db.print(25);

		
//		ArrayList<Integer> to_del=new ArrayList<Integer>();
//		ArrayList<String> comparators =new ArrayList<String>();
//		comparators.add("contain");
//		ArrayList<ArrayList<Object>> tocompare=new ArrayList<ArrayList<Object>>();
//		ArrayList<Object> buffertocompare= new ArrayList<Object> ();
//		buffertocompare.add(new String("goldenrod"));
//		tocompare.add(buffertocompare);
//		ArrayList<ArrayList<Object>> ref=new ArrayList<ArrayList<Object>>();
//		ref.add(new ArrayList<Object>(db.get_column("P_NAME")));
//		db.filter(ref, comparators, tocompare);
//		db.print(5);
		
	}
}
	

