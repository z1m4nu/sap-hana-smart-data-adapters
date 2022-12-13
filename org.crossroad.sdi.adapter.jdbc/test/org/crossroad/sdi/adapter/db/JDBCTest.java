package org.crossroad.sdi.adapter.db;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;

import org.junit.jupiter.api.Test;

public class JDBCTest {

	@Test
	public void checkPGSQL()
	{
		try {
			 Class.forName("org.postgresql.Driver");
			  Connection conn = DriverManager.getConnection("jdbc:postgresql://ban08062.tally-weijl.ch:5432/","postgres","Crossroad1");
			  checkDriver(conn);
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}
	
	@Test
	public void checkMSSQL()
	{
		try {
			 Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			  Connection conn = DriverManager.getConnection("jdbc:sqlserver://serverName=BAVSW014146.tally-weijl.ch;databaseName=C8IO","plm-job","plmJ0b2021");
			  checkDriver(conn);
		} catch(Exception e)
		{
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
	
	@Test
	public void checkSingleStore()
	{
		try {
			 Class.forName("com.singlestore.jdbc.Driver");
			  Connection conn = DriverManager.getConnection("jdbc:singlestore://bavsl014147.tally-weijl.ch:3307/replication","replicator","TWeijl2016");
			  checkDriver(conn);
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}

	@Test
	public void checkMySQL()
	{
		try {
			 Class.forName("com.mysql.cj.jdbc.Driver");
			  Connection conn = DriverManager.getConnection("jdbc:mysql://db-magento.tally-weijl.ch:3306/tally_db_new","basel-magento-backuper","y72j0ash51hka7g1f");
			  checkDriver(conn);
		} catch(Exception e)
		{
			fail(e.getMessage());
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
