/**
 * 
 */
package org.crossroad.sdi.adapter.db.mysql;

import org.crossroad.sdi.adapter.db.IDBInfo;
import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;

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
		return "MySQL Server";
	}

	@Override
	public String getMappingFile() {
		return "mysql-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return JDBCSQLRewriter.class.getCanonicalName();
	}


}
