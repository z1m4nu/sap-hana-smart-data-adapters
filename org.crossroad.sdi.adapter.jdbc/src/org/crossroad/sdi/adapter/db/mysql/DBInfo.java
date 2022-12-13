/**
 * 
 */
package org.crossroad.sdi.adapter.db.mysql;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {


	@Override
	public String getDisplayName() {
		return "MySQL Server";
	}

	@Override
	public String getMappingFile() {
		return "mysql-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return MySQLRewriter.class.getCanonicalName();
	}


}
