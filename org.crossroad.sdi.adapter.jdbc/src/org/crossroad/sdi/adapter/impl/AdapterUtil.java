/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Formatter;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.NodeType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;

/**
 * @author e.soden
 *
 */
public class AdapterUtil {

	/**
	 * 
	 */
	protected AdapterUtil() {
	}


	public static String removeSingleQuote(String str) {
		return str.replace("\'", "");
	}

	/**
	 * 
	 * @param main
	 * @param urlTemplate
	 * @return
	 * @throws AdapterException
	 */
	public static String parseJDBCUrl(PropertyGroup main, String urlTemplate) throws AdapterException {
		String option = "";
		Formatter fmt = new Formatter();
		String jdbcUrl = null;

		if (main.getPropertyEntry(AdapterConstants.KEY_OPTION) != null) {
			option = main.getPropertyEntry(AdapterConstants.KEY_OPTION).getValue();
			if (option != null && !option.isEmpty()) {

				option = ';' + option;
			}
		}

		try {
			fmt = fmt.format(urlTemplate, main.getPropertyEntry(AdapterConstants.KEY_HOSTNAME).getValue(),
					main.getPropertyEntry(AdapterConstants.KEY_PORT).getValue(),
					main.getPropertyEntry(AdapterConstants.KEY_DATABASE).getValue(), (option != null) ? option : "");

			jdbcUrl = fmt.toString();
		} finally {
			fmt.close();
		}
		return jdbcUrl;
	}

	
	public static String dumpResultSet(ResultSet rs) {
		StringBuffer buffer = new StringBuffer("Parsing table content\n");
		try {
			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			
			
			for (int index=0;index<columnCount;index++)
			{
				buffer.append("\t - RS Column ["+meta.getColumnName(index + 1) + "] value ["+rs.getString(index + 1)+"]\n");
			}
			
			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return buffer.toString();
	}
	
	public static NodeType strToNodeType(String value)
	{
		NodeType type = NodeType.NT_UNKNOWN;
		
		if ("VIEW".equalsIgnoreCase(value))
		{
			type = NodeType.VIEW;
		} else if ("TABLE".equalsIgnoreCase(value))
		{
			type = NodeType.TABLE;
		} else if ("FUNCTION".equalsIgnoreCase(value))
		{
			type = NodeType.FUNCTION;
		} else if ("PROCEDURE".equalsIgnoreCase(value))
		{
			type = NodeType.PROCEDURE;
		} else {
			type = NodeType.NT_UNKNOWN;
		}
		
		
		return type;
	}
}
