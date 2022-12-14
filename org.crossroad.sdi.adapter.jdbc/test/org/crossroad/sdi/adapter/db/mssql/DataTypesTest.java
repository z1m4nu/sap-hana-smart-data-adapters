package org.crossroad.sdi.adapter.db.mssql;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.ColumnBuilder;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;
import org.junit.jupiter.api.Test;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.ColumnCapability;

public class DataTypesTest {

	@Test
	public void checkMSSQL() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			Connection conn = DriverManager.getConnection(
					"jdbc:sqlserver://;serverName=BASW0030.tally-weijl.ch;databaseName=TW_LOGISTICS_BASEL;encrypt=false",
					"HANA", "sql2twl1");
			checkDriver(conn);

			ColumnBuilder cBuilder = new ColumnBuilder();
			cBuilder.loadMapping("mssql-mapping.properties", null);

			UniqueNameTools tools = UniqueNameTools.build("\"TW_LOGISTICS_BASEL.INFORMATION_SCHEMA.PARAMETERS\"");

			checkinfo(conn, tools);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void checkSingleStoreAsMySQL()
	{
		try {
			 Class.forName("com.mysql.cj.jdbc.Driver");
			  Connection conn = DriverManager.getConnection("jdbc:mysql://bavsl014147.tally-weijl.ch:3307/replication","replicator","TWeijl2016");
			  checkDriver(conn);
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}

	private void checkinfo(Connection connection, UniqueNameTools tools) throws Exception {
		DatabaseMetaData meta = connection.getMetaData();

		displayResultSet("Table info", meta.getTables(tools.getCatalog(), tools.getSchema(), tools.getTable(), null));

		displayResultSet("Pseudo columns", meta.getPseudoColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null));

		displayResultSet("Version columns", meta.getVersionColumns(tools.getCatalog(), tools.getSchema(), tools.getTable()));

		displayResultSet("List Imported keys", meta.getImportedKeys(tools.getCatalog(), tools.getSchema(), tools.getTable()));
		
		displayResultSet("List Primary keys", meta.getPrimaryKeys(tools.getCatalog(), tools.getSchema(), tools.getTable()));

		displayResultSet("List Exported keys", meta.getExportedKeys(tools.getCatalog(), tools.getSchema(), tools.getTable()));

		displayResultSet("List columns for table", meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null));
		
		displayResultSet("List index info", meta.getIndexInfo(tools.getCatalog(), tools.getSchema(), tools.getTable(), false,
					true));
	}

	private void displayResultSet(String title, ResultSet rs) throws Exception {
		try {

			ResultSetMetaData rsMeta = rs.getMetaData();
			StringBuilder builder = new StringBuilder();
			
			System.out.println(String.format("---> %s", title));
			
			boolean first = true;
			for (int i = 1; i < rsMeta.getColumnCount(); i++) {
				if(first)
				{
					first = false;
				} else {
					builder.append(";");
				}
				
				builder.append(rsMeta.getColumnName(i));
					
			}
			
			builder.append("\n");
			
			while (rs.next()) {

				first = true;
				for (int i = 1; i < rsMeta.getColumnCount(); i++) {
					if(first)
					{
						first = false;
					} else {
						builder.append(";");
					}
					builder.append(rs.getString(rsMeta.getColumnName(i)));
				}

				builder.append("\n");
				
			}
			System.out.println(builder.toString());
			
		} finally {
			rs.close();
		}

	}


	private void checkDriver(Connection conn) throws Exception {
		StringBuilder builder = new StringBuilder("Driver result\n");

		DatabaseMetaData meta = conn.getMetaData();

		builder.append("\t- Driver: ").append(conn.getClientInfo().toString()).append("\n");
		builder.append("\t- Database\n ");
		builder.append("\t\t- Product name:").append(meta.getDatabaseProductName()).append("\n");
		builder.append("\t\t- Product version:").append(meta.getDatabaseProductVersion()).append("\n");
		builder.append("\t\t- Major version:").append(meta.getDatabaseMajorVersion()).append("\n");
		builder.append("\t\t- Minor version:").append(meta.getDatabaseMinorVersion()).append("\n");

		System.out.println(builder.toString());
	}

}
