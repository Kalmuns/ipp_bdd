package ipp_bdd;

import java.beans.IndexedPropertyChangeEvent;
import java.nio.Buffer;
import java.security.DrbgParameters.Reseed;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilePermission;
import java.io.FileReader;
import javax.sql.rowset.Joinable;
import java.lang.Math.*;
import java.lang.Thread;

public class DataTable {
	protected boolean type;
	protected ArrayList<Object> row;
	protected ArrayList<Object> column;
	protected ArrayList<String> columnName;
	protected String tableName;

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

	public DataTable filter() {
		// to complete

		//
		DataTable result = new DataTable();
		return result;
	}

	public DataTable hashJoin(DataTable table_A, DataTable table_B, ArrayList<String> on_columnName, Object condition) {
		DataTable join_table = new DataTable();
		// To Complete

		//
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
			ArrayList<ArrayList<String>> columnList = new ArrayList<ArrayList<String>>(filenameStrings.size());
			for(int i=0; i<filenameStrings.size(); i++) {
				columnList.add(new ArrayList<String>(columnSizes[i]));
				String file = path+filenameStrings.get(i)+"_";
				for(int j=0; j<=columnSizes[i]; j++) {
					String final_file = file + Integer.toString(j) + ".tbl";
					columnList.get(i).add(final_file);
				}
			}
			
			ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
			for(int i=0; i<Parameters.Max_Threads; i++) {
				threads.add(new Thread());	
			}
			for(int i=0; i<columnList.size(); i++){
				for(int j=0; j<columnList.get(i).size(); j++) {
					int x = 0;
					
				}
			}
			
			
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
	}
	

	public void load(String string) {
		// appel load Array list
		// utilises les constantes en paramètre pour upload la table souhaiter
		// directement
		// set title de Database
	}

	public void save() {

	}

	public void print(int row_number) {

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
