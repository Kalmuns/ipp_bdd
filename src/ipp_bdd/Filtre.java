package ipp_bdd;

import java.util.ArrayList;

public class Filtre implements  Runnable {
	
	private ArrayList<Integer> to_del_index;
	public ArrayList<Object> to_filter;
	
	public Filtre(ArrayList<Integer> to_del,ArrayList<Object> to_filter) {
		to_del_index=to_del;
		this.to_filter=to_filter;
	}
	
	public void run() {
		for(Integer x = to_filter.size() - 1; x > 0; x--)// For Each row 
		{
		    if ( to_del_index.contains(x)) { // If we have to supress the index 
		    	to_filter.remove(x);
		    }
		}
	}
}

