/**
 * 
 */
package org.crossroad.sdi.adapter.db.mssql;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {

	/**
	 * 
	 */
	public DBInfo() {
		
	}

	@Override
	public String getDisplayName() {
		return "Microsoft SQL Server";
	}

	@Override
	public String getMappingFile() {
		return "mssql-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return SQLRewriter.class.getCanonicalName();
	}

}
