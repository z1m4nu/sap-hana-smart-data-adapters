/**
 * 
 */
package org.crossroad.sdi.adapter.db.mssql.v16x;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {


	@Override
	public String getDisplayName() {
		return "Microsoft SQL Server v16.x+";
	}

	@Override
	public String getMappingFile() {
		return "mssql-v16x-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return MSSQLV16xRewriter.class.getCanonicalName();
	}

}
