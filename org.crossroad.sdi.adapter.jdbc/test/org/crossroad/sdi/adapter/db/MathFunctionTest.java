package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

public class MathFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();

	String data = "SELECT COS(65), COSH(12) \"cos\" from t" ;
	
	@Test
	public void charTest() {
		try {
			System.out.println(String.format("INPUT [%s]", data));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(data)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(data)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(data)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(data)));
		} catch (Exception e) {
			fail(e);
		}
	}


}
