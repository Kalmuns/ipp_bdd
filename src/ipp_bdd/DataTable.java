package ipp_bdd;

import java.beans.IndexedPropertyChangeEvent;
import java.security.DrbgParameters.Reseed;
import java.util.ArrayList;

import javax.sql.rowset.Joinable;

public class DataTable {
	//   private boolean type;
	//	 private ArrayList<Object> row;
	 private ArrayList<Object> column;
	 private ArrayList<String> columnName;
	 private String tableName;
	 public DataTable() {
		// TODO Auto-generated constructor stub
		column= new ArrayList<>();
		columnName= new ArrayList<>();
	}
	
	 public DataTable( ArrayList<Object> column_buffer,ArrayList<String> columnName_buffer){
		column=column_buffer;
		columnName=columnName_buffer;
		
	 }
	 
	 public DataTable( ArrayList<Object> column_buffer,ArrayList<String> columnName_buffer, String tableName_buffer){
			column=column_buffer;
			columnName=columnName_buffer;
			tableName=tableName_buffer;
		 }
	 // Probably need A
	 public  DataTable select( ArrayList<String> select_columnName) 
	 {
		 ArrayList<Object> column_buffer= new ArrayList<>();
		 ArrayList<String> columnName_buffer=new ArrayList<>();
		 for (int i=0;i<select_columnName.size();i++)
				 {
				    column_buffer.add(  this.column.get(this.get_column_index(select_columnName.get(i) )));
				    columnName_buffer.add(select_columnName.get(i));
				 }
		 DataTable result= new DataTable(column_buffer,columnName_buffer);
		 return   result;
	 }
	 
	 public DataTable filer()
	 {
		//to complete 
		 
		 //
		 DataTable result= new DataTable();
		 return result;
	 }
	 
	 
	 public  DataTable hashJoin(DataTable table_A, DataTable table_B, ArrayList<String> on_columnName, Object condition)
	 {
		 DataTable join_table= new DataTable();
		 //To Complete 
		 
		 
		 //
		 return join_table;
	 }
	 
	 
	 // Return an arraylist of integer corresponding to the index of column based on their name
	 private ArrayList<Integer> get_columns_index(ArrayList<String> name) {
		 ArrayList<Integer> index = new ArrayList<>();
		 for (int i=0;i<name.size();i++)
			 if (columnName.contains(name.get(i)))
			 {
				 index.add(columnName.indexOf(name.get(i))) ;
			 }
		 return index;
	 }
	 
	 private int get_column_index(String name) {
		 int index=-1;
			 if (columnName.contains(name))
			 {
				 index=columnName.indexOf(name) ;
			 }
		 return index ;
	 }
	 
	 public ArrayList<Object> get_column (String column_name) {
		 ArrayList<Object> result = new ArrayList<>();
		 result=(ArrayList<Object>) column.get(this.get_column_index(column_name));
		 return result;
	 }
	 
	 
	 public void load() {
		 
	 }
	 
	 public void save() {
		 
	 }
	 
	 public void print(int row_number){
		 
	 }
	 
	 private void set_column(){
		 
	 }
	 
	 private void set_columns() {
	
	 }
	 
}
