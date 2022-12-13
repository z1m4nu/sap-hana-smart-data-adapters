package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.postgresql.PGSQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;

public class DateFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();
	private PGSQLRewriter pgrewriter = new PGSQLRewriter();
	
	String input = "SELECT TO_DATE ('2009-12-05', 'YYYY-MM-DD'), ADD_DAYS (TO_DATE ('2009-12-05', 'YYYY-MM-DD'), 30), ADD_DAYS (TO_DATE ('2009-12-05', 'YYYY-MM-DD'), -30) from dummy";
	
	String current = "SELECT CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIME from t";
	@Test
	public void dateAddHJDBC() {
		try {
			System.out.println(String.format("INPUT [%s]", input));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(input)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(input)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(input)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(input)));
			System.out.println(String.format("[PGSQL] OUTPUT [%s]", pgrewriter.rewriteSQL(input)));
		} catch (Exception e) {
			fail(e);
		}
	}

}
