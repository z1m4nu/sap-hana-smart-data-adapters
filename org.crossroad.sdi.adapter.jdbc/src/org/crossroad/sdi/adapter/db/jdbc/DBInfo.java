/**
 * 
 */
package org.crossroad.sdi.adapter.db.jdbc;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {

	@Override
	public String getDisplayName() {
		return "Generic JDBC";
	}

	@Override
	public String getMappingFile() {
		return "jdbc-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return JDBCSQLRewriter.class.getCanonicalName();
	}

}
