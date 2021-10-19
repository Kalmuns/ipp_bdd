package ipp_bdd;

import java.beans.IndexedPropertyChangeEvent;
import java.nio.Buffer;
import java.security.DrbgParameters.Reseed;
import java.util.ArrayList;

import javax.sql.rowset.Joinable;

public class DataTable {
	 protected boolean type;
	 protected ArrayList<Object> row;
	 protected ArrayList<Object> column;
	 protected ArrayList<String> columnName;
	 protected String tableName;
	 public DataTable() {
		// TODO Auto-generated constructor stub
		  row=new ArrayList<Object>();
		  column= new ArrayList<Object>();
		  columnName=new ArrayList<String>() ;
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
	 
	 public DataTable filter()
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
	 protected ArrayList<Integer> get_columns_index(ArrayList<String> name) {
		 ArrayList<Integer> index = new ArrayList<>();
		 for (int i=0;i<name.size();i++)
			 if (columnName.contains(name.get(i)))
			 {
				 index.add(columnName.indexOf(name.get(i))) ;
			 }
		 return index;
	 }
	 
	 protected int get_column_index(String name) {
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
	 
	 
	 public void load(ArrayList<String> filenameStrings, boolean type_buffer) {
		 type=type_buffer;
		 if (type_buffer)// Si column database
		 {
			 
		 }
		 else {// si row database ( filename est censé être de taille 1)
			
			 ArrayList<Object> rowbuffer= new ArrayList<Object>();
			 rowbuffer.add(new Integer(5));
			 rowbuffer.add(new String("blabl"));
			 row.add(rowbuffer);
		}
		 // une fois instance Database déclaré 
		 // Une arraylist objet dans column 
		 // le nom du fichier ( ou une partie du nom ) nom de la colonne 
	 }
	 
	 public void load(String string) {
		 // appel load Array list
		 // utilises les constantes en paramètre pour upload la table souhaiter directement 
		 // set title de Database 
	 }
	 
	 public void save() {
		 
	 }
	 
	 public void print(int row_number){
		 
	 }
	 
	 protected void set_column(){
		 
	 }
	 
	 protected void set_columns() {
	
	 }
	 
}
