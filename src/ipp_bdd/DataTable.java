package ipp_bdd;
import java.beans.IndexedPropertyChangeEvent;
import java.nio.Buffer;
import java.security.DrbgParameters.NextBytes;
import java.security.DrbgParameters.Reseed;
import java.time.Year;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilePermission;
import java.io.FileReader;

import javax.print.attribute.Size2DSyntax;
import javax.sql.rowset.Joinable;
import java.lang.Math.*;
import java.lang.Thread;

public class DataTable {
	protected boolean type;
	protected ArrayList<Object> row;
	protected ArrayList<Object> column;
	protected ArrayList<String> columnName;
	protected String tableName;
	protected ArrayList<String> columnList ;
	public DataTable() {
		// TODO Auto-generated constructor stub
		row = new ArrayList<Object>();
		column = new ArrayList<Object>();
		columnName = new ArrayList<String>();
	}
	public DataTable(ArrayList<Object> column_buffer, ArrayList<String> columnName_buffer) {
		column = column_buffer;
		columnName = columnName_buffer;
	}
	public DataTable(ArrayList<Object> column_buffer, ArrayList<String> columnName_buffer, String tableName_buffer) {
		column = column_buffer;
		columnName = columnName_buffer;
		tableName = tableName_buffer;
	}
	// Probably need A
	public DataTable select(ArrayList<String> select_columnName) {
		ArrayList<Object> column_buffer = new ArrayList<>();
		ArrayList<String> columnName_buffer = new ArrayList<>();
		for (int i = 0; i < select_columnName.size(); i++) {
			column_buffer.add(this.column.get(this.get_column_index(select_columnName.get(i))));
			columnName_buffer.add(select_columnName.get(i));
		}
		DataTable result = new DataTable(column_buffer, columnName_buffer);
		return result;
	}

	
	
	public DataTable filter(ArrayList<Object> object_to_compare,ArrayList<String> comparators,ArrayList<Object> reference) { // 
		// to complete
		ArrayList<Integer> index_to_del=new ArrayList<Integer>();	
		ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
		ArrayList<ArrayList<Boolean>> booleans= new ArrayList<ArrayList<Boolean>>();
		for(int i=0; i<threads.size(); i++) {
			threads.add(new Thread());	
		}
		
		for(int i=0;i<threads.size();i++){
			threads.get(i).run();
		}
		for(int i=0;i<comparators.size();i++) {
			booleans.add(new ArrayList<Boolean>());
		}
		int iter=0;//iterator
		while(iter!=comparators.size()) {
			//if((comparators.size()-iter) <threads.size()) {
			   for (int i=0;iter<threads.size();i++) {
				   if(iter<comparators.size()) {
					 threads.set(i, new Thread(new Comparator((ArrayList<Object>) object_to_compare.get(iter), comparators.get(iter), (ArrayList<Object>) reference.get(iter), booleans.get(i))))  ;
					 threads.get(i).start();
				   }
				   
				   iter++;
			   }
				for(int i=0;i<threads.size();i++) {
					try {
						threads.get(i).join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			//}
//			else {
//				for(int i=0;i<threads.size();i++) {
//					threads.set(i, new Thread(new Comparator(object_to_compare, tableName, reference, null)))
//				}
//				for(int i=0;i<threads.size();i++) {
//					try {
//						threads.get(i).join();
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//			}
			
		}
		boolean buffer=true;
		for(int i=0;i<booleans.get(0).size();i++)
		{
			buffer=true;
			for(int y=0;y<booleans.size();y++) {
				if(booleans.get(y).get(i)==false&&buffer==true) {
					buffer=false;
				}
			}
			if(buffer==false) {
				index_to_del.add(i);
			}
		}
		
		this.delete_row(index_to_del);
		//DataTable result = new DataTable();	
		
		return this;
	}	
	
	public void invert_row(int rowa,int rowb) {
		for (int i=0;i<column.size();i++) {
			Object buffer= ((ArrayList<Object>) column.get(i)).get(rowa);
			((ArrayList<Object>) column.get(i)).set(rowa,((ArrayList<Object>) column.get(i)).get(rowb));
			((ArrayList<Object>) column.get(i)).set(rowb,buffer);
		}
	}

	public DataTable project(ArrayList<String> column_toproject) {// Input is column name of the column we want to keep
		// Duplicate élimination 
		for (int i=0;i< columnName.size();i++)
		{
			if (column_toproject.contains(columnName.get(i))) {	
				
			}
			else {
				column.remove(i);
				columnName.remove(i);
				i--;
			}
		}
		
		return this;
	}
	public DataTable hashJoin(DataTable table_A, DataTable table_B, ArrayList<String> on_columnName, Object condition) {
		DataTable join_table = new DataTable();
		// To Complete
		return join_table;
	}
	// Join as we consider joining on the primary key of this table. 
	// On the columntable1 & 2 we consider the column to keep for the result
	// Fort both of them index 0 must be the column on wich join will be perform
	public DataTable sortjoin(DataTable table_tojoin, ArrayList<String>  columntable1, ArrayList<String>  columntable2, Object condition) {
		DataTable join_table = new DataTable();
		
		// sorton index
	//	@SuppressWarnings
		this.sort(columntable1.get(0));// 0 index is the name of the join column
		table_tojoin.sort(columntable2.get(0));// "" ""
	//	new QuickSort((ArrayList<Object>) this.column.get(this.get_column_index(table1)));
		//
		int leftit=0,rightit=0;// iterators of my join columns 
		ArrayList<Object> newcolonne = new ArrayList<Object>(); // Pour les nouvelles colonones : - joincolumn - this.colonne puis colonne to join
		//ArrayList<Object> newrow= new ArrayList<Object>();
		ArrayList<Object> newcolumnname= new ArrayList<Object>();
		for(int i=0;i<columntable1.size();i++) {
			newcolumnname.add(columntable1.get(i));
		}
		for (int i=1;i<columntable2.size();i++){
			newcolumnname.add(columntable2.get(i));
		}
		if(row.isEmpty()) {
			int sizetablel = ((ArrayList<Object>) this.column.get(0)).size();
			int sizetabler=table_tojoin.get_column(columntable2.get(0)).size();
			while (leftit <  sizetablel && rightit<  sizetabler) {
				if (this.get_column(columntable1.get(0)).get(leftit) == table_tojoin.get_column(columntable2.get(0)).get(rightit) ) {// A check
					for (int i =0;i<columntable1.size();i++){
						newcolonne.add(this.get_column(columntable1.get(i)).get(leftit));
					}
					for (int i=1;i<columntable2.size();i++) {
						newcolonne.add(this.get_column(columntable2.get(i)).get(rightit));
					}
				}
				if((Integer)this.get_column(columntable1.get(0)).get(leftit) <=(Integer) table_tojoin.get_column(columntable2.get(0)).get(rightit)){
					leftit++;
				}
				else {
					rightit++;
				}
				
			}
		}
		else if (column.isEmpty()) {
			// Si row join a coder ici 
		}
		
		return join_table;
	}

	// Return an arraylist of integer corresponding to the index of column based on
	// their name
	protected ArrayList<Integer> get_columns_index(ArrayList<String> name) {
		ArrayList<Integer> index = new ArrayList<>();
		for (int i = 0; i < name.size(); i++)
			if (columnName.contains(name.get(i))) {
				index.add(columnName.indexOf(name.get(i)));
			}
		return index;
	}
	protected void sort(String colonne) {
		this.column.get(this.get_column_index(colonne));
		
		// Implement quicksort from https://www.youtube.com/watch?v=Fiot5yuwPAg ? 
		// Implement for row but not other ?
		
	}
	protected int get_column_index(String name) {
		int index = -1;
		if (columnName.contains(name)) {
			index = columnName.indexOf(name);
		}
		return index;
	}
	public ArrayList<Object> get_column(String column_name) {
		ArrayList<Object> result = new ArrayList<>();
		result = (ArrayList<Object>) column.get(this.get_column_index(column_name));
		return result;
	}
	public void load(String path, ArrayList<String> filenameStrings, boolean type_buffer, ArrayList<String> type_columns) {
		type = type_buffer;
		//columnName=filenameStrings;
		if (type_buffer)// Si column database
		{
			// filenameStrings est du type ["P_BRAND", "P_COMMENT", "P_CONTAINER"]
			int[] columnSizes = new int[filenameStrings.size()];
			for(int colnb=0; colnb<filenameStrings.size(); colnb++) {
				columnSizes[colnb] = 0;
			}
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			for(int i = 0; i<listOfFiles.length; i++) {
				String file = listOfFiles[i].toString();
				String[] split_file = file.split("/");
				file = split_file[split_file.length-1];
				String[] split_file_2 = file.split("_");
				file = split_file_2[0]+"_"+split_file_2[1];
				int fileNumber = Integer.parseInt(split_file_2[2].split("\\.")[0]);
				int pos = filenameStrings.indexOf(file);
				if(pos>-1) {
					columnSizes[pos] = Math.max(columnSizes[pos], fileNumber);
				}
			}
			//columnList = new ArrayList<ArrayList<String>>(filenameStrings.size());
			columnList = new ArrayList<String>(filenameStrings.size());
			
			for(int i=0; i<filenameStrings.size(); i++) {
			//	columnList.add(new ArrayList<String>(columnSizes[i]));
				String file = path+filenameStrings.get(i)+"_";
				for(int j=0; j<=columnSizes[i]; j++) {
					String final_file = file + Integer.toString(j) + ".tbl";
					//columnList.get(i).add(final_file);
					columnList.add(final_file);
				}
			}
			
			ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
			for(int i=0; i<Parameters.Max_Threads; i++) {
				threads.add(new Thread());	
			}
			Boolean loading=false;
			while(!loading) // A check pas sur que ça marche 
			{
				if(columnList.size() <=Parameters.Max_Threads) {
					for(int i=0; i<columnList.size(); i++){
//						for (int y=0;y<columnList.get(i).size();y++)
//						threads.set(i,new Thread(Reader(columnList.get(y),this.column));
						this.column.add(new ArrayList<Object>());
						threads.set(i, new Thread(new Reader(columnList.get(i),(ArrayList<Object>) this.column.get((this.column.size()-1)))));
						//threads.get(i).start();
						
					}
					for (int y=0;y<threads.size();y++) {
						threads.get(y).run();
					}
					try {
						for (int y=0;y<threads.size();y++) {
							threads.get(y).join();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				loading=true;
				}
				else{
					for(int i=0;i<Parameters.Max_Threads;i++)
					{
						this.column.add(new ArrayList<Object>());
						threads.set(i, new Thread(new Reader(columnList.get(i),(ArrayList<Object>) this.column.get((this.column.size()-1)))));
					}
					for (int y=0;y<threads.size();y++) {
						threads.get(y).run();
					}
					try {
						for (int y=0;y<threads.size();y++) {
							threads.get(y).join();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for(int y=0;y<Parameters.Max_Threads;y++) {
						columnList.remove(0);
					}
				}
				if(columnList.isEmpty()) {
					loading=true;
				}
			System.out.println("column sier"+ column.size());
			}
			
			
//			for(int i=0; i<columnList.size(); i++){
//				if(columnList.size() <=Parameters.Max_Threads) {
//					for int y=0;y<
//				}
			// This is for partitionned colomne on row
//				for(int j=0; j<columnList.get(i).size(); j++) {
//					
//				}
				
				
			//}
			
			
		} else {
			assert filenameStrings.size() == 1; // si row database, filename est censé être de taille 1
			String current_line;
			String ext = ".tbl";
			String file_path = path.concat(filenameStrings.get(0)).concat(ext);
			String[] current_line_split;
			try {
				BufferedReader br = new BufferedReader(new FileReader(file_path));
				tableName = filenameStrings.get(0);
				while ((current_line = br.readLine()) != null) {
					current_line_split = current_line.split("[|]");
					ArrayList<Object> rowbuffer = new ArrayList<Object>();
					for(int i=0; i<type_columns.size(); i++) {
						if(type_columns.get(i).equals("int")) {
							rowbuffer.add(Integer.parseInt(current_line_split[i]));
						}
						if(type_columns.get(i).equals("string")) {
							rowbuffer.add(current_line_split[i]);
						}
						if(type_columns.get(i).equals("float")) {
							rowbuffer.add(Float.parseFloat(current_line_split[i]));
						}
					}
					row.add(rowbuffer);
				}
				if (br != null)
					br.close();
			} catch (Exception e) {
				System.out.println("File Not Found : ".concat(file_path));
			}
		}
		
		System.out.println("end of load");
	}
// If we implement row by row 	
//	public void delete_row(ArrayList<Integer> to_del_index)
//	{
//		for(Integer x = ((ArrayList<Object>) column.get(0)).size() - 1; x > 0; x--)// For Each row 
//		{
//		    if ( to_del_index.contains(x)) { // If we have to supress the index 
//		    	for(int y=0;y<column.size();y++){// For All the Table
//		    		((ArrayList<Object>) column.get(y)).remove(x);
//		    	}
//		    }
//		}
//	}
	
	public void delete_row(ArrayList<Integer> to_del_index)
	{
		ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
		for(int i=0; i<Parameters.Max_Threads; i++) {
			threads.add(new Thread());	
		}
		boolean deleting_finish=false;
		int iter=0;//iterator
		while(!deleting_finish)// Check boucle infinie 
		{
			if (threads.size()<=column.size()){
				for(int y=iter ;y<column.size(); y++) {
				//	threads.set(i, null)
					threads.set(y, new Thread(new Filtre(to_del_index, (ArrayList<Object>) column.get(y))));
					threads.get(y).start();
				}
				for(int i=0;i<threads.size();i++) {
					try {
						threads.get(i).join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			deleting_finish=true;
			}
			else {
				for(int i=iter;i<threads.size();i++) {
					//	threads.set(i, null)
						threads.set(i, new Thread(new Filtre(to_del_index, (ArrayList<Object>) column.get(i))));
						threads.get(i).start();
						iter++;
					}
					for(int i=0;i<threads.size();i++) {
						try {
							threads.get(i).join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				if(iter==column.size())
				{
					deleting_finish=true;
				}
			}
		}
	}
	
	
	public void load(String string) {
		// appel load Array list
		// utilises les constantes en paramètre pour upload la table souhaiter
		// directement
		// set title de Database
	}

	public void save() {

	}

	public void print(int nrow) {
		String ligne =  "";
		ArrayList<Object> arrayList = (ArrayList<Object>) column.get(0);
		int buffer=arrayList.size();
		System.out.println("print \n size of table "+buffer );
		for (int i=0;i<columnName.size();i++) {
			ligne.concat(columnName.get(i));
			ligne.concat(" / ");
		}
		System.out.println(ligne);
		int i=0;
		for(i=0;i<nrow;i++){
			ligne="";
			for(int y=0;y<column.size();y++){
				//((ArrayList<Object>)	column.get(y)).get(i);
				ligne=ligne.concat(((ArrayList<Object>) column.get(y)).get(i).toString());
				ligne=ligne.concat(" / ");
				
			}
			System.out.println(ligne);
		}
	}

	protected void set_column() {

	}

	protected void set_columns() {

	}

	public static void main(String[] args) {
		/*
		String path = "50Mo/";
		ArrayList<String> filenameStrings = new ArrayList<String>();
		filenameStrings.add("customer");
		boolean type_buffer = false;
		ArrayList<String> type_columns = new ArrayList<String>();
		type_columns.add("int");
		type_columns.add("string");
		type_columns.add("string");
		type_columns.add("int");
		type_columns.add("string");
		type_columns.add("float");
		type_columns.add("string");
		type_columns.add("string");
		DataTable db = new DataTable();
		*/
		
		// db.load(path, filenameStrings, type_buffer, type_columns);
		//System.out.println(db.tableName);
		//System.out.println(db.row.get(0));
		
		/*
		String path = "50MoColumns/";
		ArrayList<String> filenameStrings = new ArrayList<String>();
		filenameStrings.add("P_BRAND");
		filenameStrings.add("P_COMMENT");
		boolean type_buffer = true;
		ArrayList<String> type_columns = new ArrayList<String>();
		DataTable db = new DataTable();
		db.load(path, filenameStrings, type_buffer, type_columns);
		*/	
	}
}
