package ipp_bdd;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ColumnStorage {

	public static void to_columns(String srcFile, String filenameString, String columnsPath,
			ArrayList<String> columnNames, int nbRows, int batchSize) {
		// Reading variables
		assert batchSize <= nbRows;
		String current_line;
		String ext = ".tbl";
		String file_path = srcFile.concat(filenameString).concat(ext);
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
						String str = columnsPath.concat(filenameString).concat("_")
								.concat(columnNames.get(column_index)).concat("_").concat(String.valueOf(fileNumber))
								.concat(ext);
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
						String str = columnsPath.concat(filenameString).concat("_")
								.concat(columnNames.get(column_index)).concat("_").concat(String.valueOf(fileNumber))
								.concat(ext);
						FileWriter writer = new FileWriter(str, true);
						for (int row = 0; row <= writingRowBatch; row++) {
							writer.write(batch[row][column_index].concat("\n"));
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
						String str = columnsPath.concat(filenameString).concat("_")
								.concat(columnNames.get(column_index)).concat("_").concat(String.valueOf(fileNumber))
								.concat(ext);
						FileWriter writer = new FileWriter(str, true);
						for (int row = 0; row <= writingRowBatch; row++) {
							writer.write(batch[row][column_index].concat("\n"));
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
					String str = columnsPath.concat(filenameString).concat("_").concat(columnNames.get(column_index))
							.concat("_").concat(String.valueOf(fileNumber)).concat(ext);
					FileWriter writer = new FileWriter(str, true);
					for (int row = 0; row <= writingRowBatch; row++) {
						writer.write(batch[row][column_index].concat("\n"));
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
		String filenameString = "part";
		String columnsPath = "50MoColumns/";
		String[] arrayColumnNames = new String[] { "PARTKEY", "NAME", "MFGR", "BRAND", "TYPE", "SIZE", "CONTAINER",
				"RETAILPRICE", "COMMENT" };
		int nbColumns = 9;
		ArrayList<String> columnNames = new ArrayList<String>(nbColumns);
		for (int i = 0; i < nbColumns; i++) {
			columnNames.add(arrayColumnNames[i]);
		}
		int nbRows = 1000;
		int batchSize = 100;
		to_columns(srcFile, filenameString, columnsPath, columnNames, nbRows, batchSize);
	}

}
