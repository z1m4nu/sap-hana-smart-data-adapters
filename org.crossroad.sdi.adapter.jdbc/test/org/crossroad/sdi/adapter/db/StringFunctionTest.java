package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;

public class StringFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private MSSQLV16xRewriter msSQLv16xrewriter = new MSSQLV16xRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();

	String charfx = "SELECT CHAR (65) || CHAR (110) || CHAR (116) \"character\" from t" ;
	String sustring = "SELECT SUBSTRING('12345',4,5), SUBSTRING('12345', 2) from dummy";
	String trimString = "SELECT TRIM('a' FROM 'aaa123456789aa'), TRIM(LEADING 'a' FROM 'aaa123456789aa'), TRIM(TRAILING 'a' FROM 'aaa123456789aa') \"trim both\" FROM DUMMY";
	
	@Test
	public void charTest() {
		try {
			System.out.println(String.format("INPUT [%s]", charfx));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(charfx)));
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	public void substring() {
		try {
			System.out.println(String.format("INPUT [%s]", sustring));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(sustring)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(sustring)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(sustring)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(sustring)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(sustring)));
		} catch (Exception e) {
			fail(e);
		}
	}
	
	@Test
	public void trimfx() {
		try {
			System.out.println(String.format("INPUT [%s]", trimString));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(trimString)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(trimString)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(trimString)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(trimString)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(trimString)));
		} catch (Exception e) {
			fail(e);
		}
	}

}
