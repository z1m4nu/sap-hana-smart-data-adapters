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
		return MSSQLRewriter.class.getCanonicalName();
	}

}
