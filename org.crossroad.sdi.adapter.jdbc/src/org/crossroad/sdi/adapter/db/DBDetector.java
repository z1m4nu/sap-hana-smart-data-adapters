/**
 * 
 */
package org.crossroad.sdi.adapter.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.crossroad.sdi.adapter.db.mssql.DBInfo;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;

/**
 * @author e.soden
 *
 */
public final class DBDetector {
	static final String MSSQL = "Microsoft SQL Server";
	static final String PGSQL = "PostgreSQL";
	static final String MYSQL = "MySQL";
	static final String SINGLESTORE = "SingleStore";
	
	private DBDetector() {
	}
	
	public static IDBInfo detect(Connection connection) throws AdapterException {
		try {

			AbstractJDBCAdapter.logger.info("Detecting database options");
			
			DatabaseMetaData meta = connection.getMetaData();
			String productName = meta.getDatabaseProductName();
			Integer majorVersion = meta.getDatabaseMajorVersion();
			Integer minorVersion = meta.getDatabaseMinorVersion();
			
			DetectedDBInfo dbInfo = null;

			if (StringUtils.hasText(productName)) {
				/*
				 * SQL Server
				 */
				if (productName.equalsIgnoreCase(MSSQL)) {
					if (majorVersion > 15) {
						dbInfo = DetectedDBInfo.factory(new DBInfo());
						dbInfo.setRewriterClass(MSSQLV16xRewriter.class.getCanonicalName());
						dbInfo.setMappingFile("mssql-v16x-mapping.properties");
					} else {
						dbInfo = DetectedDBInfo.factory(new DBInfo());
					}
				} else if (productName.equalsIgnoreCase(PGSQL)) {
					dbInfo = DetectedDBInfo.factory(new org.crossroad.sdi.adapter.db.postgresql.DBInfo());
				} else if (productName.equalsIgnoreCase(MYSQL)) {
					dbInfo = DetectedDBInfo.factory(new org.crossroad.sdi.adapter.db.mysql.DBInfo());
				} else if (productName.equalsIgnoreCase(SINGLESTORE)) {
					dbInfo = DetectedDBInfo.factory(new org.crossroad.sdi.adapter.db.singlestore.DBInfo());
				} else {
					dbInfo = DetectedDBInfo.factory(new org.crossroad.sdi.adapter.db.jdbc.DBInfo());
				}
			} else {
				throw new AdapterException("Unable to determine the proper SQL rewriter switch to manual mode");
			}

			AbstractJDBCAdapter.logger.debug(dbInfo.toString());
			
			return dbInfo;
		} catch (AdapterException e) {
			throw e;
		} catch (Exception e) {
			throw new AdapterException(e);
		}
	}
	
	
	private static class DetectedDBInfo implements IDBInfo {
		private String displayName;
		private String mappingFile;
		private String rewriterClass;
		
		
		
		public DetectedDBInfo(String displayName, String mappingFile, String rewriterClass) {
			this.displayName = displayName;
			this.mappingFile = mappingFile;
			this.rewriterClass = rewriterClass;
		}

		/**
		 * @param displayName the displayName to set
		 */
		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}

		/**
		 * @param mappingFile the mappingFile to set
		 */
		public void setMappingFile(String mappingFile) {
			this.mappingFile = mappingFile;
		}

		/**
		 * @param rewriterClass the rewriterClass to set
		 */
		public void setRewriterClass(String rewriterClass) {
			this.rewriterClass = rewriterClass;
		}

		@Override
		public String getDisplayName() {
			return this.displayName;
		}

		@Override
		public String getMappingFile() {
			return this.mappingFile;
		}

		@Override
		public String getRewriterClass() {
			return this.rewriterClass;
		}
		
		
		public static DetectedDBInfo factory(IDBInfo defaut) {
			return new DetectedDBInfo(defaut.getDisplayName(), defaut.getMappingFile(), defaut.getRewriterClass());
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("\t- Display name [").append(getDisplayName()).append("\n\t- Mapping file [").append(getMappingFile()).append("]\n\t- Rewriter [").append(rewriterClass).append("]\n");
			return builder.toString();
		}
		
	}
}
