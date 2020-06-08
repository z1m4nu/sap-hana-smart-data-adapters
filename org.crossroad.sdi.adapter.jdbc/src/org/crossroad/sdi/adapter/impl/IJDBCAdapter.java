/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;

/**
 * @author e.soden
 *
 */
public interface IJDBCAdapter {

	/**
	 * Build the JDBC url according to the driverClass
	 * 
	 * @param driverClass
	 * @return
	 */
	public String getJdbcUrl(PropertyGroup main) throws AdapterException;

   /**
	 * 
	 * @return
	 */
	public Class getLoggerName();
	
	public void onClose();
	

	/**
	 * 
	 * @throws AdapterException
	 */
	public void doCloseResultSet() throws AdapterException;


}
