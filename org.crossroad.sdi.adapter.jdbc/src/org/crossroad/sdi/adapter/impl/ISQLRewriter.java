package org.crossroad.sdi.adapter.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.postgresql.PGSQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

public interface ISQLRewriter {

	static final String MSSQL = "Microsoft SQL Server";
	static final String PGSQL = "PostgreSQL";
	static final String MYSQL = "MySQL";
	static final String SINGLESTORE = "SingleStore";

	String rewriteSQL(String sql) throws AdapterException;

	public ExpressionBase.Type getQueryType();

	public void setMaxIndentifierLength(int maxIdentifierLength);

	public void addSchemaAliasReplacement(String schemaAlias, String schemaAliasReplacement);

	public static ISQLRewriter factory(Connection connection) throws AdapterException {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			String productName = meta.getDatabaseProductName();
			Integer majorVersion = meta.getDatabaseMajorVersion();
			Integer minorVersion = meta.getDatabaseMinorVersion();

			if (StringUtils.hasText(productName)) {
				/*
				 * SQL Server
				 */
				if (productName.equalsIgnoreCase(MSSQL)) {
					if (majorVersion > 15) {
						return new MSSQLV16xRewriter();
					} else {
						return new MSSQLRewriter();
					}
				} else if (productName.equalsIgnoreCase(PGSQL)) {
					return new PGSQLRewriter();
				} else if (productName.equalsIgnoreCase(MYSQL)) {
					return new MySQLRewriter();
				} else if (productName.equalsIgnoreCase(SINGLESTORE)) {
					return new SingleStoreRewriter();
				}
			} else {
				throw new AdapterException("Unable to determine the proper SQL rewriter switch to manual mode");
			}

			throw new AdapterException(String.format(
					"No SQL rewriter for '%s' with verion major '%d' minor '%d' switching to manual mode.", productName,
					majorVersion, minorVersion));
		} catch (AdapterException e) {
			throw e;
		} catch (Exception e) {
			throw new AdapterException(e);
		}
	}
}