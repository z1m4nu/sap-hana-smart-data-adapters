/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;

/**
 * @author e.soden
 *
 */
public final class AdapterConstants {
	public static final String KEY_THIRDPARTY = "jdbc.thirdparty.custom";
	public static final String KEY_JDBC_JAR = "jdbc.jarfile";
	public static final String KEY_JDBC_URL = "jdbc.url";
	public static final String KEY_JDBC_DRIVERCLASS = "jdbc.driver.class";

	public static final String KEY_GROUP_MAIN = "jdbc.main";
	public static final String KEY_GROUP_NORMAL = "jdbc.normal";
	public static final String KEY_GROUP_CONNECTION = "jdbc.connection";
	public static final String KEY_GROUP_DATAMAPPING = "jdbc.datamapping";

	

	public static final String KEY_HOSTNAME = "jdbc.host";
	public static final String KEY_PORT = "jdbc.port";
	public static final String KEY_DATABASE = "jdbc.dbname";
	public static final String KEY_OPTION = "jdbc.option";
	public static final String KEY_WITHSYS = "jdbc.systables";
	public static final String KEY_NULLASEMPTYSTRING = "jdbc.nullasemptystring";
	
	public static final String KEY_DATAMAPPING = "jdbc.datamapping.custom";
	public static final String KEY_DATAMAPPING_FILE = "jdbc.datamapping.file";
	public static final String KEY_DATAMAPPING_FILE_DEFAULT = "mapping.properties";
	
	public static final String KEY_JDBC_TYPE = "jdbc.db.type";
	public static final String KEY_JDBC_TYPE_DEFAULT = JDBCSQLRewriter.class.getCanonicalName();
	
	
	public static final String PRP_SCHEMA = "schema";
	public static final String PRP_CATALOG = "catalog";
	public static final String PRP_TABLE = "table";
	public static final String PRP_UNIQNAME = "uniquename";
	public static final String BOOLEAN_TRUE = "true";
	public static final String BOOLEAN_FALSE = "false";

	public static final String NULL_AS_STRING = "<none>";

	public static final int CATALOG_EXPANDED = 0;
	public static final int SCHEMA_EXPANDED = 1;
	public static final int TABLE_EXPANDED = 2;
	public static final int NULL_EXPANDED = -1;

	public static final String FORMAT_FULLDATE = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String FORMAT_DATEONLY = "yyyy-MM-dd";
	public static final String FORMAT_TIMEONLY = "HH:mm:ss.SSS";
	public static final String KEY_THREADPOOL_SIZE = "jdbc.threadpool.size";

	public static final String REMOTESOURCE_VERSION = "1.0.1";

	/**
	 * 
	 */
	private AdapterConstants() {
		// TODO Auto-generated constructor stub
	}

}
