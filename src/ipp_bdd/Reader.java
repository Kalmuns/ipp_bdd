package ipp_bdd;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Reader implements Runnable{
	public String filename;
	public ArrayList<Object> document;
	
	public Reader(String filename, ArrayList<Object> document) {
		this.filename = filename;
		this.document = document;
	}

	
	@Override
	public void run() {
		String current_line;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while ((current_line = br.readLine()) != null) {
				
				if(Parameters.typeMap.get(filename.split("\\.")[0])=="int")
					this.document.add(Integer.parseInt(current_line));
				else if(Parameters.typeMap.get(filename.split("\\.")[0])=="float")
					this.document.add(Float.parseFloat(current_line));
				else if(Parameters.typeMap.get(filename.split("\\.")[0])=="string")
					this.document.add(current_line);
			}
			if (br != null)
				br.close();
		} catch (Exception e) {
			System.out.println("File Not Found : ".concat(filename));
		}
	}
	
	
}
