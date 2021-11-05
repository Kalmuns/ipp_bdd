package ipp_bdd;

import java.util.ArrayList;

public class Test {
	 String path = "50MoColumns/";
	 boolean type_buffer = true;
	 ArrayList<String> type_columns = new ArrayList<String>();
	public Test() {

	}
	
	public void  rowvscolumnload() {
		
		DataTable columnDataTable=new DataTable();
		ArrayList<String> toload=new ArrayList<String>();
		toload.add("P_PARTKEY");
		toload.add("P_NAME");
		toload.add("P_RETAILPRICE");
		 
	    long time=  System.nanoTime();
		columnDataTable.load(path, toload, type_buffer, type_columns);
	    time=System.nanoTime()-time;
	    System.out.print("time in nanoseconds = "+time);
	   
	    DataTable rowTable= new DataTable();
	    time=  System.nanoTime();
	    rowTable.load("50Mo/", toload, false, type_columns);
	    
	    time=System.nanoTime()-time;
	    System.out.print("time in nanoseconds = "+time);
	    //columnDataTable.print(50);
	}
	
	public void filtertest(){
		DataTable columnDataTable=new DataTable();
		ArrayList<String> toload=new ArrayList<String>();
		toload.add("P_PARTKEY");
		toload.add("P_NAME");
		toload.add("P_RETAILPRICE");
		columnDataTable.load(path, toload, type_buffer, type_columns);
		columnDataTable.print(5);
		ArrayList<Integer> to_del=new ArrayList<Integer>();
		ArrayList<String> comparators =new ArrayList<String>();
		comparators.add("<");
		ArrayList<ArrayList<Object>> tocompare=new ArrayList<ArrayList<Object>>();
		ArrayList<Object> buffertocompare= new ArrayList<Object> ();
		buffertocompare.add(new Float(904));
		tocompare.add(buffertocompare);
		ArrayList<ArrayList<Object>> ref=new ArrayList<ArrayList<Object>>();
		ref.add(new ArrayList<Object>(columnDataTable.get_column("P_RETAILPRICE")));
		columnDataTable.filter(ref, comparators, tocompare);
		columnDataTable.print(5);
		
	}
	
	public void grouptest() {
		DataTable columnDataTable=new DataTable();
		ArrayList<String> toload=new ArrayList<String>();
		toload.add("P_NAME");
		toload.add("P_TYPE");
		toload.add("P_BRAND");
		toload.add("P_RETAILPRICE");
		columnDataTable.load(path, toload, type_buffer, type_columns);
		columnDataTable.print(5);
		
		ArrayList<String> column_togroup = new ArrayList<String>(2);
		column_togroup.add("P_BRAND");
		column_togroup.add("P_TYPE");
		
		ArrayList<String> column_tokeep = new ArrayList<String>(4);
		column_tokeep.add("P_NAME");
		column_tokeep.add("P_BRAND");
		column_tokeep.add("P_TYPE");
		column_tokeep.add("P_RETAILPRICE");
		
		ArrayList<String> aggregation = new ArrayList<String>(4);
		aggregation.add("concat");
		aggregation.add("first");
		aggregation.add("first");
		aggregation.add("avg");
		columnDataTable.groupBy(column_togroup, column_tokeep, aggregation);
		
		columnDataTable.print(5);
	}
	
	public void jointest() {
		
	}
	
}

