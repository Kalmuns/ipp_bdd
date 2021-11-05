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
import javax.swing.GroupLayout.Group;

import java.lang.Math.*;
import java.lang.Thread;

public class DataTable {
	protected boolean type;
	protected ArrayList<Object> row;
	protected ArrayList<Object> column;
	protected ArrayList<String> columnName;
	protected String tableName;
	protected ArrayList<String> columnList;
	ArrayList<ArrayList<Object>> booleans;
	ArrayList<Integer> buffer_sort;

	// ArrayList<ArrayList<Boolean>> booleans;
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

	public DataTable groupBy(ArrayList<String> column_togroup, ArrayList<String> aggregation) {
		ArrayList<String> groupkey = new ArrayList<String>(((ArrayList<Object>) column.get(0)).size());
		for (int i = 0; i < ((ArrayList<Object>) column.get(0)).size(); i++) {
			groupkey.add(null);
		}
		String buffer_groupkey;
		ArrayList<Integer> id_column_togroup = new ArrayList<Integer>();
		for (int i = 0; i < column_togroup.size(); i++) {
			id_column_togroup.add(this.get_column_index(column_togroup.get(i)));
		}
		for (int i = 0; i < ((ArrayList<Object>) column.get(0)).size(); i++) {
			buffer_groupkey = "";
			for (int j = 0; j < id_column_togroup.size(); j++) {
				buffer_groupkey = buffer_groupkey
						.concat(((ArrayList<Object>) column.get(id_column_togroup.get(j))).get(i).toString());
			}
			groupkey.set(i, buffer_groupkey);
		}
		column.add(groupkey);
		columnName.add("groupkey");
		this.sort("groupkey");
/*<<<<<<< HEAD
			
		
		
		
		int i=1;
		int compteur=0;
		ArrayList<Object> restoftable= new ArrayList<Object>();
		for( i=0;i<column_tokeep.size();i++)
		{
			restoftable.add(null);
		}
//			while (i<((ArrayList<Object>) this.column.get(0)).size() && this.get_column("groupkey").get(i).equals(this.get_column("groupkey").get(i-1))) {
//				
//			}
		
		
		for( i=1;i<((ArrayList<Object>) column.get(0)).size();i++) {
			if(this.get_column("groupkey").get(i).equals(this.get_column("groupkey").get(i-1))) {
				compteur++; 
				if(restoftable.get(0) ==null)
					for( int y=0;y<column_tokeep.size();y++)
					{
						
						//restoftable.set(y, buffer);
					}
				else {
					//restoftable.
				}
			}
			else {
				
				compteur=0;
				for( int y=0;y<column_tokeep.size();y++)
				{
					restoftable.set(y, null);
				}
				
				
=======*/

		ArrayList<Object> temp_table = new ArrayList<Object>();
		for (int i = 0; i < column.size() - 1; i++) {
			temp_table.add(null);
		}
		int final_pos = 0;
		while (final_pos < ((ArrayList<Object>) this.column.get(0)).size()) {
			int temp_pos = 0;
			int temp_count = 1;
			for (int i = 0; i < column.size() - 1; i++) {
				temp_table.set(i, ((ArrayList<Object>) column.get(i)).get(final_pos));
			}
			while (final_pos + temp_pos + 1 < ((ArrayList<Object>) this.column.get(0)).size()
					&& ((ArrayList<Object>) column.get(column.size() - 1)).get(final_pos + temp_pos).equals(
							((ArrayList<Object>) column.get(column.size() - 1)).get(final_pos + temp_pos + 1))) {
				for (int i = 0; i < column.size() - 1; i++) {
					if (aggregation.get(i).equals("concat")) {
						temp_table.set(i, ((String) temp_table.get(i))
								.concat((String) ((ArrayList<Object>) column.get(i)).get(final_pos + temp_pos + 1)));
					}
					if (aggregation.get(i).equals("sum") || aggregation.get(i).equals("average")) {
						if (temp_table.get(i) instanceof Integer) {
							temp_table.set(i, ((Integer) temp_table.get(i))
									+ (Integer) ((ArrayList<Object>) column.get(i)).get(final_pos + temp_pos + 1));
						}
						if (temp_table.get(i) instanceof Float) {
							temp_table.set(i, ((Float) temp_table.get(i))
									+ (Float) ((ArrayList<Object>) column.get(i)).get(final_pos + temp_pos + 1));
						}
					}
				}
				temp_pos++;
				temp_count++;
			}
			for (int i = 0; i < column.size() - 1; i++) {
				if (aggregation.get(i).equals("average")) {
					if (temp_table.get(i) instanceof Integer) {
						temp_table.set(i, ((Integer) temp_table.get(i)) / temp_count);
					}
					if (temp_table.get(i) instanceof Float) {
						temp_table.set(i, ((Float) temp_table.get(i)) / temp_count);
					}

				}
				((ArrayList<Object>) this.column.get(i)).set(final_pos, temp_table.get(i));
			}
			ArrayList<Integer> to_delete = new ArrayList<Integer>();
			for (int j = final_pos + 1; j < final_pos + temp_pos + 1; j++) {
				to_delete.add(j);
			}
			if (to_delete.size() > 0) {
				this.delete_row(to_delete);
			}
			final_pos++;
		}
		return this;
	}

	public DataTable filter(ArrayList<ArrayList<Object>> object_to_compare, ArrayList<String> comparators,
			ArrayList<ArrayList<Object>> reference) { //
		// to complete
		ArrayList<Integer> index_to_del = new ArrayList<Integer>();
		ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
		// booleans= new ArrayList<ArrayList<Boolean>>();
		booleans = new ArrayList<ArrayList<Object>>();
		for (int i = 0; i < Parameters.Max_Threads; i++) {
			threads.add(new Thread());
		}
		for (int i = 0; i < comparators.size(); i++) {
			// booleans.add(new ArrayList<Boolean>());
			booleans.add(new ArrayList<Object>());
		}
		int iter = 0;// iterator
		while (iter < comparators.size()) {
			// if((comparators.size()-iter) <threads.size()) {
			for (int i = 0; iter < threads.size(); i++) {
				if (iter < comparators.size()) {
					threads.set(i, new Thread(new Comparator((ArrayList<Object>) object_to_compare.get(iter),
							comparators.get(iter), (ArrayList<Object>) reference.get(iter), booleans.get(i))));
				}
				iter++;
			}
			for (int i = 0; i < threads.size(); i++) {
				threads.get(i).run();
			}
			for (int i = 0; i < threads.size(); i++) {
				try {
					threads.get(i).join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// }
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
		boolean buffer = true;
		for (int i = 0; i < booleans.get(0).size(); i++) {
			buffer = true;
			for (int y = 0; y < booleans.size(); y++) {
				if (((Boolean) booleans.get(y).get(i)) == false) {
					buffer = false;
				}
			}
			if (buffer == true) {
				index_to_del.add(i);
			}
		}

		this.delete_row(index_to_del);
		// DataTable result = new DataTable();
		booleans = null;
		return this;
	}

	public void invert_row(int rowa, int rowb, int index_column_unmodified) {
		for (int i = 0; i < column.size(); i++) {
			if (i != index_column_unmodified) {
				Object buffer = ((ArrayList<Object>) column.get(i)).get(rowa);
				((ArrayList<Object>) column.get(i)).set(rowa, ((ArrayList<Object>) column.get(i)).get(rowb));
				((ArrayList<Object>) column.get(i)).set(rowb, buffer);
			}

		}
	}

	public void permute_rows(int index_column_unmodified) {
		ArrayList<Object> temp_column = new ArrayList<Object>(((ArrayList<Object>) column.get(0)).size());
		for (int j = 0; j < ((ArrayList<Object>) column.get(0)).size(); j++) {
			temp_column.add(null);
		}
		for (int i = 0; i < column.size(); i++) {
			if (i != index_column_unmodified) {
				for (int j = 0; j < buffer_sort.size(); j++) {
					temp_column.set(j, (((ArrayList<Object>) column.get(i)).get(buffer_sort.get(j))));
				}
				for (int j = 0; j < ((ArrayList<Object>) column.get(0)).size(); j++) {
					((ArrayList<Object>) column.get(i)).set(j, temp_column.get(j));
				}
			}
		}
	}

	public DataTable project(ArrayList<String> column_toproject) {// Input is column name of the column we want to keep
		// Duplicate élimination
		for (int i = 0; i < columnName.size(); i++) {
			if (column_toproject.contains(columnName.get(i))) {

			} else {
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

	
//	 public DataTable sortjoin(DataTable table_tojoin, ArrayList<String>
//	 columntable1, ArrayList<String> columntable2, Object condition) { DataTable
//	 join_table = new DataTable();
//	 
//	  // sorton index // @SuppressWarnings this.sort(columntable1.get(0));// 0
//	  index is the name of the join column
//	  table_tojoin.sort(columntable2.get(0));// "" "" // new
//	  QuickSort((ArrayList<Object>)
//	  this.column.get(this.get_column_index(table1))); // int leftit=0,rightit=0;//
//	  iterators of my join columns ArrayList<Object> newcolonne = new
//	  ArrayList<Object>(); // Pour les nouvelles colonones : - joincolumn -
//	  this.colonne puis colonne to join //ArrayList<Object> newrow= new
//	  ArrayList<Object>(); ArrayList<Object> newcolumnname= new
//	  ArrayList<Object>(); for(int i=0;i<columntable1.size();i++) {
//	  newcolumnname.add(columntable1.get(i)); } for (int
//	  i=1;i<columntable2.size();i++){ newcolumnname.add(columntable2.get(i)); }
//	  if(row.isEmpty()) { int sizetablel = ((ArrayList<Object>)
//	  this.column.get(0)).size(); int
//	  sizetabler=table_tojoin.get_column(columntable2.get(0)).size(); while (leftit
//	  < sizetablel && rightit< sizetabler) { if
//	  (this.get_column(columntable1.get(0)).get(leftit) ==
//	  table_tojoin.get_column(columntable2.get(0)).get(rightit) ) {// A check for
//	  (int i =0;i<columntable1.size();i++){
//	  newcolonne.add(this.get_column(columntable1.get(i)).get(leftit)); } for (int
//	  i=1;i<columntable2.size();i++) {
//	 * newcolonne.add(this.get_column(columntable2.get(i)).get(rightit)); } }
//	 * if((Integer)this.get_column(columntable1.get(0)).get(leftit) <=(Integer)
//	 * table_tojoin.get_column(columntable2.get(0)).get(rightit)){ leftit++; } else
//	 * { rightit++; }
//	 * 
//	 * } } else if (column.isEmpty()) { // Si row join a coder ici }
//	 * 
//	 * return join_table; }
//	 */
	// colonne condition are applied on first colonne in colonne table
	public DataTable sortjoin(DataTable table_tojoin, ArrayList<String>	 columntable1, ArrayList<String> columntable2, String condition,boolean alreadyjoin) {
		
		if(!alreadyjoin) {
			this.sort(columntable1.get(0));
			table_tojoin.sort(columntable2.get(0));
		}
		int leftindex=0,rightindex=0;
		
		ArrayList<Object> newcolonne = new ArrayList<Object>(); // Pour les nouvelles colonones : - joincolumn - this.colonne puis colonne to join
		for(int i=0;i<(columntable1.size()+columntable2.size()-1);i++) {
			newcolonne.add(new ArrayList<Object>());
		}
		//ArrayList<Object> newrow= new ArrayList<Object>();
		ArrayList<String> newcolumnname= new ArrayList<String>();
		for(int i=0;i<columntable1.size();i++) {
			newcolumnname.add(columntable1.get(i));
		}
		for (int i=1;i<columntable2.size();i++){
			newcolumnname.add(columntable2.get(i));
		}
		
		int lefttable_size=((ArrayList<Object>) column.get(0)).size();
		int righttable_size=table_tojoin.get_column(0).size();
	
		
		while(leftindex<lefttable_size||rightindex<righttable_size) {

			
					if (this.get_column(columntable1.get(0)).get(leftindex).equals(table_tojoin.get_column(columntable2.get(0)).get(rightindex)) ) {// si objet égaux
						for (int i =0;i<columntable1.size();i++){
							((ArrayList<Object>) newcolonne.get(i)).add(this.get_column(columntable1.get(i)).get(leftindex));
						}
						for (int i=1;i<columntable2.size();i++) {
							((ArrayList<Object>) newcolonne.get(columntable1.size()+i-1)).add(table_tojoin.get_column(columntable2.get(i)).get(rightindex));
						}
					}
					
					if(leftindex<lefttable_size-1&&rightindex<righttable_size-1)
						if( this.get_column(0).get(0) instanceof Integer) {
							if( ((Integer)  this.get_column(0).get(leftindex)) <= ((Integer)  table_tojoin.get_column(0).get(rightindex))) {
								leftindex++;
							}
							else {
								rightindex++;
							}
						}
						
						if( this.get_column(0).get(0) instanceof Float) {
							if( ((Float)  this.get_column(0).get(leftindex)) <= ((Float)  table_tojoin.get_column(0).get(rightindex))) {
								leftindex++;
							}
							else {
								rightindex++;
							}
						}
						if( this.get_column(0).get(0) instanceof String) {
							if( ((String)  this.get_column(0).get(leftindex)).compareTo(((String)  table_tojoin.get_column(0).get(rightindex))) <=0) {
								leftindex++;
							}
							else {
								rightindex++;
							}
						}
					
					else if(leftindex<lefttable_size-1) {
						leftindex++;
					}
					else if (rightindex<righttable_size-1){
						rightindex++;
					}
					else if(!(leftindex<lefttable_size-1 && rightindex<righttable_size-1)) {
						leftindex++;
						rightindex++;
					}
		}
		
		
		DataTable  join_table= new DataTable(newcolonne, newcolumnname);
		return join_table;
	}
	public ArrayList<Object> get_column(int index){
		return (ArrayList<Object>) column.get(index);
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

	protected void merge(int col_idx, int left_idx, int middle_idx, int right_idx) {
		int left_len = middle_idx - left_idx + 1;
		int right_len = right_idx - middle_idx;
		ArrayList<Object> left_temp = new ArrayList<Object>(left_len);
		ArrayList<Object> right_temp = new ArrayList<Object>(right_len);
		ArrayList<Integer> left_temp_buffer = new ArrayList<Integer>(left_len);
		ArrayList<Integer> right_temp_buffer = new ArrayList<Integer>(right_len);
		for (int i = 0; i < left_len; i++) {
			left_temp.add(((ArrayList<Object>) this.column.get(col_idx)).get(left_idx + i));
			left_temp_buffer.add(this.buffer_sort.get(left_idx + i));
		}
		for (int i = 0; i < right_len; i++) {
			right_temp.add(((ArrayList<Object>) this.column.get(col_idx)).get(middle_idx + 1 + i));
			right_temp_buffer.add(this.buffer_sort.get(middle_idx + 1 + i));
		}
		int left_moving_idx = 0;
		int right_moving_idx = 0;
		int total_moving_idx = left_idx;
		while (left_moving_idx < left_len && right_moving_idx < right_len) {
			if (left_temp.get(0) instanceof Integer) {
				if (((Integer) left_temp.get(left_moving_idx))
						.intValue() <= ((Integer) right_temp.get(right_moving_idx)).intValue()) {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							left_temp.get(left_moving_idx));
					this.buffer_sort.set(total_moving_idx, new Integer(left_temp_buffer.get(left_moving_idx)));
					left_moving_idx++;
				} else {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							right_temp.get(right_moving_idx));
					this.buffer_sort.set(total_moving_idx, new Integer(right_temp_buffer.get(right_moving_idx)));
					right_moving_idx++;
				}
				total_moving_idx++;
			}
			if (left_temp.get(0) instanceof Float) {
				if (((Float) left_temp.get(left_moving_idx)).floatValue() <= ((Float) right_temp.get(right_moving_idx))
						.floatValue()) {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							left_temp.get(left_moving_idx));
					this.buffer_sort.set(total_moving_idx, left_temp_buffer.get(left_moving_idx));
					left_moving_idx++;
				} else {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							right_temp.get(right_moving_idx));
					this.buffer_sort.set(total_moving_idx, right_temp_buffer.get(right_moving_idx));
					right_moving_idx++;
				}
				total_moving_idx++;
			}
			if (left_temp.get(0) instanceof String) {
				if (((String) (left_temp.get(left_moving_idx)))
						.compareTo(((String) right_temp.get(right_moving_idx))) <= 0) {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							left_temp.get(left_moving_idx));
					this.buffer_sort.set(total_moving_idx, left_temp_buffer.get(left_moving_idx));
					left_moving_idx++;
				} else {
					((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx,
							right_temp.get(right_moving_idx));
					this.buffer_sort.set(total_moving_idx, right_temp_buffer.get(right_moving_idx));
					right_moving_idx++;
				}
				total_moving_idx++;
			}
		}
		while (left_moving_idx < left_len) {
			((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx, left_temp.get(left_moving_idx));
			this.buffer_sort.set(total_moving_idx, left_temp_buffer.get(left_moving_idx));
			left_moving_idx++;
			total_moving_idx++;
		}
		while (right_moving_idx < right_len) {
			((ArrayList<Object>) this.column.get(col_idx)).set(total_moving_idx, right_temp.get(right_moving_idx));
			this.buffer_sort.set(total_moving_idx, right_temp_buffer.get(right_moving_idx));
			right_moving_idx++;
			total_moving_idx++;
		}
	}

	protected void sort(String col_to_order, int left_idx, int right_idx) {
		int col_idx = this.get_column_index(col_to_order);
		if (left_idx < right_idx) {
			int middle_idx = left_idx + (right_idx - left_idx) / 2;
			sort(col_to_order, left_idx, middle_idx);
			sort(col_to_order, middle_idx + 1, right_idx);
			merge(col_idx, left_idx, middle_idx, right_idx);
		}

	}

	protected void sort(String col_to_order) {
		int length = ((ArrayList<Object>) column.get(0)).size();
		buffer_sort = new ArrayList<Integer>(length);
		for (int i = 0; i < length; i++) {
			buffer_sort.add(new Integer(i));
		}
		sort(col_to_order, 0, length - 1);
		permute_rows(this.get_column_index(col_to_order));
		buffer_sort = null;
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

	public void load(String path, ArrayList<String> filenameStrings, boolean type_buffer,
			ArrayList<String> type_columns) {
		type = type_buffer;

		columnName = filenameStrings;
		// columnName=filenameStrings;
		if (type_buffer)// Si column database
		{
			// filenameStrings est du type ["P_BRAND", "P_COMMENT", "P_CONTAINER"]
			int[] columnSizes = new int[filenameStrings.size()];
			for (int colnb = 0; colnb < filenameStrings.size(); colnb++) {
				columnSizes[colnb] = 0;
			}
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				String file = listOfFiles[i].toString();
				String[] split_file = file.split("/");
				file = split_file[split_file.length - 1];
				String[] split_file_2 = file.split("_");
				file = split_file_2[0] + "_" + split_file_2[1];
				int fileNumber = Integer.parseInt(split_file_2[2].split("\\.")[0]);
				int pos = filenameStrings.indexOf(file);
				if (pos > -1) {
					columnSizes[pos] = Math.max(columnSizes[pos], fileNumber);
				}
			}
			// columnList = new ArrayList<ArrayList<String>>(filenameStrings.size());
			columnList = new ArrayList<String>(filenameStrings.size());

			for (int i = 0; i < filenameStrings.size(); i++) {
				// columnList.add(new ArrayList<String>(columnSizes[i]));
				String file = path + filenameStrings.get(i) + "_";
				for (int j = 0; j <= columnSizes[i]; j++) {
					String final_file = file + Integer.toString(j) + ".tbl";
					// columnList.get(i).add(final_file);
					columnList.add(final_file);
				}
			}

			ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
			for (int i = 0; i < Parameters.Max_Threads; i++) {
				threads.add(new Thread());
			}
			Boolean loading = false;
			while (!loading) // A check pas sur que ça marche
			{
				if (columnList.size() <= Parameters.Max_Threads) {
					for (int i = 0; i < columnList.size(); i++) {
//						for (int y=0;y<columnList.get(i).size();y++)
//						threads.set(i,new Thread(Reader(columnList.get(y),this.column));
						this.column.add(new ArrayList<Object>());
						threads.set(i, new Thread(new Reader(columnList.get(i),
								(ArrayList<Object>) this.column.get((this.column.size() - 1)))));
						// threads.get(i).start();

					}
					for (int y = 0; y < threads.size(); y++) {
						threads.get(y).run();
					}
					try {
						for (int y = 0; y < threads.size(); y++) {
							threads.get(y).join();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					loading = true;
				} else {
					for (int i = 0; i < Parameters.Max_Threads; i++) {
						this.column.add(new ArrayList<Object>());
						threads.set(i, new Thread(new Reader(columnList.get(i),
								(ArrayList<Object>) this.column.get((this.column.size() - 1)))));
					}
					for (int y = 0; y < threads.size(); y++) {
						threads.get(y).run();
					}
					try {
						for (int y = 0; y < threads.size(); y++) {
							threads.get(y).join();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for (int y = 0; y < Parameters.Max_Threads; y++) {
						columnList.remove(0);
					}
				}
				if (columnList.isEmpty()) {
					loading = true;
				}
				System.out.println("column sier" + column.size());
			}

//			for(int i=0; i<columnList.size(); i++){
//				if(columnList.size() <=Parameters.Max_Threads) {
//					for int y=0;y<
//				}
			// This is for partitionned colomne on row
//				for(int j=0; j<columnList.get(i).size(); j++) {
//					
//				}

			// }

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
					for (int i = 0; i < type_columns.size(); i++) {
						if (type_columns.get(i).equals("int")) {
							rowbuffer.add(Integer.parseInt(current_line_split[i]));
						}
						if (type_columns.get(i).equals("string")) {
							rowbuffer.add(current_line_split[i]);
						}
						if (type_columns.get(i).equals("float")) {
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

	public void delete_row(ArrayList<Integer> to_del_index) {
		ArrayList<Thread> threads = new ArrayList<Thread>(Parameters.Max_Threads);
		for (int i = 0; i < Parameters.Max_Threads; i++) {
			threads.add(new Thread());
		}
		boolean deleting_finish = false;
		int iter = 0;// iterator
		while (!deleting_finish)// Check boucle infinie
		{
			if (threads.size() >= (column.size() - iter)) {
				for (int y = 0; y < threads.size(); y++) {
					// threads.set(i, null)
					if (iter < column.size()) {
						threads.set(y, new Thread(new Filtre(to_del_index, (ArrayList<Object>) column.get(iter))));
					}
					else {
						threads.set(y, null);
					}
					iter++;
				}

				for (int i = 0; i < threads.size(); i++) {
					if(threads.get(i)!=null) {
						threads.get(i).run();
					}
					
				}

				for (int i = 0; i < threads.size(); i++) {
					try {
						if(threads.get(i)!=null) {
							threads.get(i).join();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				deleting_finish = true;
			} else {
				for (int i = 0; i < threads.size(); i++) {
					// threads.set(i, null)
					threads.set(i, new Thread(new Filtre(to_del_index, (ArrayList<Object>) column.get(iter))));
					iter++;
				}

				for (int i = 0; i < threads.size(); i++) {
					threads.get(i).run();
				}
				for (int i = 0; i < threads.size(); i++) {
					try {
						threads.get(i).join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (iter == column.size()) {
					deleting_finish = true;
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
		String ligne = "";
		ArrayList<Object> arrayList = (ArrayList<Object>) column.get(0);
		int buffer = arrayList.size();
		System.out.println("print \n size of table " + buffer);
		for (int i = 0; i < columnName.size(); i++) {
			ligne.concat(columnName.get(i));
			ligne.concat(" / ");
		}
		System.out.println(ligne);
		int i = 0;
		for (i = 0; i < nrow; i++) {
			ligne = "";
			for (int y = 0; y < column.size(); y++) {
				// ((ArrayList<Object>) column.get(y)).get(i);
				ligne = ligne.concat(((ArrayList<Object>) column.get(y)).get(i).toString());
				ligne = ligne.concat(" / ");

			}
			System.out.println(ligne);
		}
	}

	protected void set_column() {

	}

	protected void set_columns() {

	}
	

	// public static void main(String[] args) {
	/*
	 * String path = "50Mo/"; ArrayList<String> filenameStrings = new
	 * ArrayList<String>(); filenameStrings.add("customer"); boolean type_buffer =
	 * false; ArrayList<String> type_columns = new ArrayList<String>();
	 * type_columns.add("int"); type_columns.add("string");
	 * type_columns.add("string"); type_columns.add("int");
	 * type_columns.add("string"); type_columns.add("float");
	 * type_columns.add("string"); type_columns.add("string"); DataTable db = new
	 * DataTable();
	 */

	// db.load(path, filenameStrings, type_buffer, type_columns);
	// System.out.println(db.tableName);
	// System.out.println(db.row.get(0));

	/*
	 * String path = "50MoColumns/"; ArrayList<String> filenameStrings = new
	 * ArrayList<String>(); filenameStrings.add("P_BRAND");
	 * filenameStrings.add("P_COMMENT"); boolean type_buffer = true;
	 * ArrayList<String> type_columns = new ArrayList<String>(); DataTable db = new
	 * DataTable(); db.load(path, filenameStrings, type_buffer, type_columns);
	 */
	// }
}
