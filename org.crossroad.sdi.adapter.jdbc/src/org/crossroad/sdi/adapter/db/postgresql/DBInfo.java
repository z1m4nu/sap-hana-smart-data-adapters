/**
 * 
 */
package org.crossroad.sdi.adapter.db.postgresql;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {


	@Override
	public String getDisplayName() {
		return "PostGreSQL Server";
	}

	@Override
	public String getMappingFile() {
		return "jdbc-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return PGSQLRewriter.class.getCanonicalName();
	}


}
