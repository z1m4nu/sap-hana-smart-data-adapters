package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

public class MiscFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private MSSQLV16xRewriter msSQLv16xrewriter = new MSSQLV16xRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();

	String coalesce = "SELECT COALESCE(null,12,16), COALESCE('test',1,2) from t" ;
	String current = "SELECT CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIME from t";
	
	@Test
	public void coalesceTest() {
		try {
			System.out.println(String.format("INPUT [%s]", coalesce));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(coalesce)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(coalesce)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(coalesce)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(coalesce)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(coalesce)));
		} catch (Exception e) {
			fail(e);
		}
	}


}
