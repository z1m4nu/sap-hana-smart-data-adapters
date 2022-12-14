/**
 * 
 */
package org.crossroad.sdi.adapter.db.table;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;

import org.crossroad.sdi.adapter.tables.TableMetadataBuilder;
import org.junit.jupiter.api.Test;

import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.TableMetadata;

/**
 * @author e.soden
 *
 */
public class TableMetaDataBuilderTest {

	@Test
	public void checkMSSQL() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			Connection connection = DriverManager.getConnection(
					"jdbc:sqlserver://;serverName=BASW0030.tally-weijl.ch;databaseName=TW_LOGISTICS_BASEL;encrypt=false",
					"HANA", "sql2twl1");
			
			TableMetadataBuilder builder = new TableMetadataBuilder();
			builder.init("mssql-mapping.properties", null);

			TableMetadata tableMeta = builder.createMetaData(connection.getMetaData(), "\"TW_LOGISTICS_BASEL.dbo.EEM_Tracking_po_v1\"");
			
			for(Column c:tableMeta.getColumns())
			{
				System.out.println(String.format("Column %s", c.getName()));
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
