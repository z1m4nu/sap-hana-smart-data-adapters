/**
 * 
 */
package org.crossroad.sdi.adapter.db.singlestore;

import org.crossroad.sdi.adapter.db.IDBInfo;

/**
 * @author e.soden
 *
 */
public class DBInfo implements IDBInfo {


	@Override
	public String getDisplayName() {
		return "SingleStore Server";
	}

	@Override
	public String getMappingFile() {
		return "mysql-mapping.properties";
	}

	@Override
	public String getRewriterClass() {
		return SingleStoreRewriter.class.getCanonicalName();
	}


}
