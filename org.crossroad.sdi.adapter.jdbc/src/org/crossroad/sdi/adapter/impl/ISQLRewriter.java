package org.crossroad.sdi.adapter.impl;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

public interface ISQLRewriter {


	String rewriteSQL(String sql) throws AdapterException;
	
	public ExpressionBase.Type getQueryType();
	
	public void setMaxIndentifierLength(int maxIdentifierLength);
	public void addSchemaAliasReplacement(String schemaAlias, String schemaAliasReplacement);
	

	

}