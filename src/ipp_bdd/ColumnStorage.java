package ipp_bdd;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ColumnStorage {

	public static void to_columns(String srcFile, String filenameString, String filenameShortcut, String columnsPath,
			ArrayList<String> columnNames, int nbRows, int batchSize) {
		// Reading variables
		assert batchSize <= nbRows;
		String current_line;
		String ext = ".tbl";
		String file_path = srcFile + filenameString + ext;
		String[] current_line_split;
		// Writing variables
		int fileNumber = 0;
		int writingRowFile = 0;
		int writingRowBatch = 0;
		int writingRowFile2 = 0;
		int writingRowBatch2 = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_path));
			String[][] batch = new String[batchSize][columnNames.size()];
			while ((current_line = br.readLine()) != null) {
				if (writingRowFile == 0) {
					for (int column_index = 0; column_index < columnNames.size(); column_index++) {
						String str = columnsPath + filenameShortcut + "_" + columnNames.get(column_index) + "_"
								+ String.valueOf(fileNumber) + ext;
						File file = new File(str);
						file.createNewFile();
					}
				}
				current_line_split = current_line.split("[|]");
				for (int s = 0; s < columnNames.size(); s++) {
					batch[writingRowBatch][s] = current_line_split[s];
				}
				if (writingRowBatch == batchSize - 1) {
					for (int column_index = 0; column_index < columnNames.size(); column_index++) {
						String str = columnsPath + filenameShortcut + "_" + columnNames.get(column_index) + "_"
								+ String.valueOf(fileNumber) + ext;
						FileWriter writer = new FileWriter(str, true);
						for (int row = 0; row <= writingRowBatch; row++) {
							writer.write(batch[row][column_index] + "\n");
						}
						writer.close();
					}
					writingRowBatch2 = 0;
					writingRowFile2 = writingRowFile + 1;
					if (writingRowFile == nbRows - 1) {
						fileNumber++;
						writingRowFile2 = 0;
					}
				} else if (writingRowBatch != batchSize - 1 && writingRowFile == nbRows - 1) {
					for (int column_index = 0; column_index < columnNames.size(); column_index++) {
						String str = columnsPath + filenameShortcut + "_" + columnNames.get(column_index) + "_"
								+ String.valueOf(fileNumber) + ext;
						FileWriter writer = new FileWriter(str, true);
						for (int row = 0; row <= writingRowBatch; row++) {
							writer.write(batch[row][column_index] + "\n");
						}
						writer.close();
					}
					writingRowBatch2 = 0;
					writingRowFile2 = 0;
					fileNumber++;
				} else {
					writingRowBatch2 = writingRowBatch + 1;
					writingRowFile2 = writingRowFile + 1;
				}

				writingRowBatch = writingRowBatch2;
				writingRowFile = writingRowFile2;
			}
			if (br != null)
				br.close();
			if (writingRowBatch > 0) {
				for (int column_index = 0; column_index < columnNames.size(); column_index++) {
					String str = columnsPath + filenameShortcut + "_" + columnNames.get(column_index) + "_"
							+ String.valueOf(fileNumber) + ext;
					FileWriter writer = new FileWriter(str, true);
					for (int row = 0; row <= writingRowBatch; row++) {
						writer.write(batch[row][column_index] + "\n");
					}
					writer.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String srcFile = "50Mo/";
		String columnsPath = "50MoColumns/";
		int nbTables = 8;
		String[] filenameStrings = new String[] { "customer", "lineitem", "nation", "orders", "part", "partsupp",
				"region", "supplier" };
		String[] filenameShortcuts = new String[] { "C", "L", "N", "O", "P", "PS", "R", "S" };
		int[] nbColumns = new int[] { 8, 16, 4, 9, 9, 5, 3, 7 };
		String[] customerColumns = new String[] { "CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
				"MKTSEGMENT", "COMMENT" };
		String[] lineitemColumns = new String[] { "ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY",
				"EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE",
				"SHIPINSTRUCT", "SHIPMODE", "COMMENT" };
		String[] nationColumns = new String[] { "NATIONKEY", "NAME", "REGIONKEY", "COMMENT" };
		String[] ordersColumns = new String[] { "ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
				"ORDERPRIORITY", "CLERK", "SHIPPRIORITY", "COMMENT" };
		String[] partColumns = new String[] { "PARTKEY", "NAME", "MFGR", "BRAND", "TYPE", "SIZE", "CONTAINER",
				"RETAILPRICE", "COMMENT" };
		String[] partsuppColumns = new String[] { "PARTKEY", "SUPPKEY", "AVAILQTY", "SUPPLYCOST", "COMMENT" };
		String[] regionColumns = new String[] { "REGIONKEY", "NAME", "COMMENT" };
		String[] supplierColumns = new String[] { "SUPPLEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
				"COMMENT" };
		String[][] arrayColumnNames = new String[][] { customerColumns, lineitemColumns, nationColumns, ordersColumns,
				partColumns, partsuppColumns, regionColumns, supplierColumns };

		for (int i = 0; i < nbTables; i++) {
			ArrayList<String> columnNames = new ArrayList<String>(nbColumns[i]);
			for (int j = 0; j < nbColumns[i]; j++) {
				columnNames.add(arrayColumnNames[i][j]);
			}
			int nbRows = Integer.MAX_VALUE;
			int batchSize = 100;
			to_columns(srcFile, filenameStrings[i], filenameShortcuts[i], columnsPath, columnNames, nbRows, batchSize);
		}
	}

}
