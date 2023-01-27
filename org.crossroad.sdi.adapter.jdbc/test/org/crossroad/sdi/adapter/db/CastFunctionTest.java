package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.postgresql.PGSQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

public class CastFunctionTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();
	private PGSQLRewriter pgrewriter = new PGSQLRewriter();
	
	String charfx = "SELECT TO_BIGINT(111223339), TO_BOOLEAN('TRUE'), TO_DATE('2022-01-12','YYYY-MM-DD'), TO_DECIMAL(10), TO_DOUBLE(10),	TO_INT(12.2),TO_INTEGER(12.2),	TO_NVARCHAR('test'),TO_REAL(0),	TO_SMALLINT(12339), TO_TINYINT(34) from t" ;
	String s1 = "SELECT \"sfs_potential\".\"field1\", TO_DECIMAL(SUM(\"sfs_potential\".\"grand_total\"), 19, 2) FROM sfs_potential \"sfs_potential\"";
	@Test
	public void toTest() {
		try {
			System.out.println(String.format("INPUT [%s]", charfx));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(charfx)));
			System.out.println(String.format("[PGSQL] OUTPUT [%s]", pgrewriter.rewriteSQL(charfx)));
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	public void toTest1() {
		try {
			System.out.println(String.format("INPUT [%s]", s1));
			System.out.println(String.format("[JDBC] OUTPUT [%s]", jdbcRewriter.rewriteSQL(s1)));
			System.out.println(String.format("[MYSQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(s1)));
			System.out.println(String.format("[MSSQL] OUTPUT [%s]", msSQLrewriter.rewriteSQL(s1)));
//			System.out.println(String.format("[SSQL] OUTPUT [%s]", singleStorerewriter.rewriteSQL(s1)));
			System.out.println(String.format("[PGSQL] OUTPUT [%s]", pgrewriter.rewriteSQL(s1)));
		} catch (Exception e) {
			fail(e);
		}
	}

}
