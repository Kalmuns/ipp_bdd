package ipp_bdd;

import java.util.ArrayList;

public class Test extends DataTable{
	public Test() {
	super();
	ArrayList<String> strings=new ArrayList<>();
	ArrayList<Integer> chiffres= new ArrayList<>();
	chiffres.add(2);
	chiffres.add(3);
	strings.add("first element");
	strings.add("second element");
	
	column.add(chiffres);
	column.add(strings);
	columnName=strings;
	
	Object objects[];
	int x=5;
	objects=new Object[x];
	System.out.println("column"+column.get(0));
		// TODO Auto-generated constructor stub
	}
	
}
