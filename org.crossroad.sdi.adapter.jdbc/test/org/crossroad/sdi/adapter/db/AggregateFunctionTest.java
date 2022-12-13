package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;

public class AggregateFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private MSSQLV16xRewriter msSQLv16xrewriter = new MSSQLV16xRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();

	String avg = "SELECT AVG(\"Price\") FROM DUMMY" ;
	String stdev = "SELECT STDDEV(\"Price\"),STDDEV_POP(A),STDDEV_SAMP(A) FROM DUMMY";
	
	@Test
	public void avgTest() {
		try {
			System.out.println(String.format("INPUT [%s]", avg));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(avg)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(avg)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(avg)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(avg)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(avg)));
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	public void stdevTest() {
		try {
			System.out.println(String.format("INPUT [%s]", stdev));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(stdev)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(stdev)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(stdev)));
			System.out.println(String.format("[MSSQL V16.x] OUTPUT [%s]", msSQLv16xrewriter.rewriteSQL(stdev)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(stdev)));
		} catch (Exception e) {
			fail(e);
		}
	}
}
