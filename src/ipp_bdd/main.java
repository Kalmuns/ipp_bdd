package ipp_bdd;

import java.util.ArrayList;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ArrayList<Object> test = new ArrayList<Object>();
		Thread thread = new Thread(new Reader("name", test));
		System.out.println(test.get(0));

	}
}
	
