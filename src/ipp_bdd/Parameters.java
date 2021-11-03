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
		
		typMap.put("S_SUPPKEY_0","int");
		typMap.put("S_NAME_0","string");
		typMap.put("S_ADDRESS_0","string");
		typMap.put("S_NATIONKEY_0","int");
		typMap.put("S_PHONE_0","string");
		typMap.put("S_ACCTBAL_0","float");
		typMap.put("S_COMMENT_0","string");
		
		typMap.put("PS_PARTKEY_0","int");
		typMap.put("PS_SUPPKEY_0","int");
		typMap.put("PS_AVAILQTY_0","int");
		typMap.put("PS_SUPPLYCOST_0","float");
		typMap.put("PS_COMMENT_0","string");
		
		typMap.put("C_CUSTKEY_0","int");
		typMap.put("C_NAME_0","string");
		typMap.put("C_ADDRESS_0","string");
		typMap.put("C_NATIONKEY_0","int");
		typMap.put("C_PHONE_0","string");
		typMap.put("C_ACCTBAL_0","float");
		typMap.put("C_MKTSEGMENT_0","string");
		typMap.put("C_COMMENT_0","string");
		
		typMap.put("O_ORDERKEY_0","int");
		typMap.put("O_CUSTKEY_0","int");
		typMap.put("O_ORDERSTATUS_0","string");
		typMap.put("O_TOTALPRICE_0","float");
		typMap.put("O_ORDERDATE_0","string");
		typMap.put("O_ORDERPRIORITY_0","string");
		typMap.put("O_CLERK_0","string");
		typMap.put("O_SHIPPRIORITY_0","int");
		typMap.put("O_COMMENT_0","string");
		
		typMap.put("L_ORDERKEY_0","int");
		typMap.put("L_PARTKEY_0","int");
		typMap.put("L_SUPPKEY_0","int");
		typMap.put("L_LINENUMBER_0","int");
		typMap.put("L_QUANTITY_0","float");
		typMap.put("L_EXTENDEDPRICE_0","float");
		typMap.put("L_DISCOUNT_0","float");
		typMap.put("L_TAX_0","float");
		typMap.put("L_RETURNFLAG_0","string");
		typMap.put("L_LINESTATUS_0","string");
		typMap.put("L_SHIPDATE_0","string");
		typMap.put("L_COMMITDATE_0","string");
		typMap.put("L_RECEIPTDATE_0","string");
		typMap.put("L_SHIPINSTRUCT_0","string");
		typMap.put("L_SHIPMODE_0","string");
		typMap.put("L_COMMENT_0","string");
		
		typMap.put("N_NATIONKEY_0","int");
		typMap.put("N_NAME_0","string");
		typMap.put("N_REGIONKEY_0","int");
		typMap.put("N_COMMENT_0","string");
		
		typMap.put("R_REGIONKEY_0","int");
		typMap.put("R_NAME_0","string");
		typMap.put("R_COMMENT_0","string");
		
		typeMap=Collections.unmodifiableMap(typMap);
	
	}
	
	
	
	
	
}
