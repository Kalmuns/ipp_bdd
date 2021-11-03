package ipp_bdd;

import java.util.ArrayList;

import javax.management.loading.PrivateClassLoader;

public class Comparator implements Runnable {
	
	private ArrayList<Object> object_to_check;
	private String comparator_type;
	private ArrayList<Object> reference;
	private ArrayList<Boolean> to_keep;//Boolean with true the index of the table wich will not be supress
	
	public Comparator(ArrayList<Object> object_to_check,String comparator_type,ArrayList<Object> reference,ArrayList<Boolean> to_keep) {
		// TODO Auto-generated constructor stub
		this.object_to_check=object_to_check;
		this.comparator_type=comparator_type;
		this.reference=reference;
	}
	
	// This function construct the to_keep boolean list
	public void run() {
		
			// If is int 
			if(((ArrayList<Object>) object_to_check.get(0)).get(0) instanceof Integer) {
				if(reference.size()>1) { // If we compare list of integer into list of integer
					if(comparator_type=="=") {
					
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() == ((Integer) reference.get(i)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="<") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() < ((Integer) reference.get(i)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type==">") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() > ((Integer) reference.get(i)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
				else if(reference.size()==1) {// If we compare all list of integer into the same integer
					if(comparator_type=="=") {
					
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() == ((Integer) reference.get(0)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="<") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() < ((Integer) reference.get(0)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type==">") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Integer) object_to_check.get(i)).intValue() > ((Integer) reference.get(0)).intValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
			}
			
			// If we look for a float
			else if( ((ArrayList<Object>) object_to_check.get(0)).get(0) instanceof Float ) {
				if(reference.size()>1) // IF we compare float to a flot
				{
					if(comparator_type=="=") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() == ((Float) reference.get(i)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="<") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() < ((Float) reference.get(i)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type==">") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() > ((Float) reference.get(i)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
				else if(reference.size()==1) {
					if(comparator_type=="=") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() == ((Float) reference.get(0)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="<") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() < ((Float) reference.get(0)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type==">") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((Float) object_to_check.get(i)).floatValue() > ((Float) reference.get(0)).floatValue() )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
			}
			else if(((ArrayList<Object>) object_to_check.get(0)).get(0) instanceof String) {
				if(reference.size()>1) {
					
				
					if(comparator_type=="=") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((String) object_to_check.get(i)).equals(((String) reference.get(i))) )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="contain") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((String) object_to_check.get(i)).contains((String) reference.get(i))) 
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
				else if (reference.size()==1) {
					if(comparator_type=="=") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((String) object_to_check.get(i)).equals(((String) reference.get(0))) )
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
					else if(comparator_type=="contain") {
						for(int i=0;i<object_to_check.size();i++) {
							if (((String) object_to_check.get(i)).contains((String) reference.get(0))) 
								to_keep.add(true);
							else 
								to_keep.add(false);
						}
					}
				}
//				else if(comparator_type==">") {
//					for(int i=0;i<object_to_check.size();i++) {
//						if (((Float) object_to_check.get(i)).floatValue() > ((Float) reference.get(i)).floatValue() )
//							to_keep.add(true);
//						else 
//							to_keep.add(false);
//					}
//				}
			}

		}
		
	
}
