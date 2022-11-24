package org.crossroad.sdi.adapter.db.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.Test;

public class MSSQLRewriterTest {


	@Test
	public void testRewriteSQL() {
		MSSQLRewriter rewriter = new MSSQLRewriter();
		try {
			String s1 = "SELECT COUNT(*) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\"";			
			String s2 = rewriter.rewriteSQL(s1);
			assertEquals("SELECT COUNT(*) FROM [CATALOG].[SCHEMA].[TABLE] A",s2 );
			
			
			System.out.println(String.format("INPUT [%s]", s1));
			System.out.println(String.format("OUTPUT [%s]", s2));
			
			
		
			s1 = "SELECT FIELD1, FIELD2, COUNT(AVG(FIELD3)) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\" GROUP BY FIELD1, FIELD2, FIELD3";
			s2 = rewriter.rewriteSQL(s1);
			assertEquals("SELECT [FIELD1], [FIELD2], COUNT(AVG([FIELD3])) FROM [CATALOG].[SCHEMA].[TABLE] A GROUP BY [FIELD1], [FIELD2], [FIELD3]", s2);
			System.out.println(String.format("INPUT [%s]", s1));
			System.out.println(String.format("OUTPUT [%s]", s2));

		} catch (Exception e) {
			fail(e);
		}
	}
	
	@Test
	public void rewriteCount() {
		MSSQLRewriter rewriter = new MSSQLRewriter();
		try {
			String s1 = "SELECT FIELD1, FIELD2, COUNT(FIELD3) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\" GROUP BY FIELD1, FIELD2, FIELD3";
			String s2 = rewriter.rewriteSQL(s1);
			assertEquals("SELECT [FIELD1], [FIELD2], COUNT([FIELD3]) FROM [CATALOG].[SCHEMA].[TABLE] A GROUP BY [FIELD1], [FIELD2], [FIELD3]", s2);

			System.out.println(String.format("INPUT [%s]", s1));
			System.out.println(String.format("OUTPUT [%s]", s2));

		} catch(Exception e)
		{
			fail(e);

		}
	}
	
	@Test
	public void rewriteCountAggregate() {
		MSSQLRewriter rewriter = new MSSQLRewriter();
		try {
			String s1 = "SELECT FIELD1, FIELD2, COUNT(AVG(FIELD3)) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\" GROUP BY FIELD1, FIELD2, FIELD3";
			String s2 = rewriter.rewriteSQL(s1);
			assertEquals("SELECT [FIELD1], [FIELD2],COUNT(AVG([FIELD3])) FROM [CATALOG].[SCHEMA].[TABLE] A GROUP BY [FIELD1], [FIELD2], [FIELD3]", s2);

			System.out.println(String.format("INPUT [%s]", s1));
			System.out.println(String.format("OUTPUT [%s]", s2));

		} catch(Exception e)
		{
			fail(e);

		}
	}
}
