package ipp_bdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Parameters {
	public final static int Max_Threads=2;
	public final static boolean row=false;
	public final static boolean column=true;
	public final static int inttype=0;
	public static final Map<String, String> typeMap;
	static {
		Map<String, String> typMap=new HashMap<String, String>();
		typMap.put("P_PARTKEY_0", "int");
		typMap.put("P_NAME_0","string");
		typMap.put("P_MFGR_0", "string");
		typMap.put("P_BRAND_0", "string");
		typMap.put("P_TYPE_0","string");
		typMap.put("P_CONTAINER_0", "string");
		typMap.put("P_RETAILPRICE_0", "float");
		typMap.put("P_SIZE_0", "int");
		typMap.put("P_COMMENT_0","string");
		
		
		
		typeMap=Collections.unmodifiableMap(typMap);
	
	}
	
	
	
	
	
}
