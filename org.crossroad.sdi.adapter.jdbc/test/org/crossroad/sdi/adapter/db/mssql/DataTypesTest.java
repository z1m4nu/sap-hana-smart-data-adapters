package org.crossroad.sdi.adapter.db.mssql;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
	public void checkMSSQL()
	{
		try {
			 Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			  Connection conn = DriverManager.getConnection("jdbc:sqlserver://;serverName=BASW0030.tally-weijl.ch;databaseName=TW_LOGISTICS_BASEL;encrypt=false","HANA","sql2twl1");
			  checkDriver(conn);
			  
			  
			  ColumnBuilder cBuilder = new ColumnBuilder();
			  cBuilder.loadMapping("mssql-mapping.properties", null);
			  
			  UniqueNameTools tools = UniqueNameTools.build("\"TW_LOGISTICS_BASEL.dbo.EEM_Tracking_po_v1\"");
			  
			  checkColumns(cBuilder, conn, tools);
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}
	
	private void checkColumns(ColumnBuilder columnBuilder, Connection connection, UniqueNameTools tools) throws Exception  {
		DatabaseMetaData meta = null;
		ResultSet rsColumns = null;
		
		 
		List<Column> cols = new ArrayList<Column>();
		try {
			
			
			 System.out.println("Create unique key list for [" + tools.getTable() + "]");
			meta = connection.getMetaData();

			rsColumns = meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null);

			while (rsColumns.next()) {
				String columnName = rsColumns.getString("COLUMN_NAME");
				int columnType = rsColumns.getInt("DATA_TYPE");
				String typeName = rsColumns.getString("TYPE_NAME");
				int size = rsColumns.getInt("COLUMN_SIZE");
				int nullable = rsColumns.getInt("NULLABLE");
				int scale = rsColumns.getInt("DECIMAL_DIGITS");

				
				Column column = columnBuilder.createColumn(columnName, columnType, typeName, size, size, scale);
				
				System.out.println(String.format("Column '%s' type '%d' type '%s' HANA Type '%s'", columnName, columnType, typeName, column.getDataType().name()));

				
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new AdapterException(e);
		} finally {
			if (rsColumns != null) {
				try {
					rsColumns.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				rsColumns = null;
			}
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
