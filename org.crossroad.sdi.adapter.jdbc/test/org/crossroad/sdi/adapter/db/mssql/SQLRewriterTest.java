package org.crossroad.sdi.adapter.db.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.Before;
import org.junit.Test;

public class SQLRewriterTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRewriteSQL() {
		SQLRewriter rewriter = new SQLRewriter();
		try {
			String s1 = "SELECT COUNT(*) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\"";			
			assertEquals("SELECT COUNT(*) FROM [CATALOG].[SCHEMA].[TABLE] A", rewriter.rewriteSQL(s1));
			
			s1 = "SELECT FIELD1, FIELD2, COUNT(FIELD3) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\" GROUP BY FIELD1, FIELD2, FIELD3";
			assertEquals("SELECT [FIELD1], [FIELD2], COUNT([FIELD3]) FROM [CATALOG].[SCHEMA].[TABLE] A GROUP BY [FIELD1], [FIELD2], [FIELD3]", rewriter.rewriteSQL(s1));

			s1 = "SELECT FIELD1, FIELD2, COUNT(AVG(FIELD3)) FROM \"CATALOG.SCHEMA.TABLE\" \"TABLE\" GROUP BY FIELD1, FIELD2, FIELD3";
			assertEquals("SELECT [FIELD1], [FIELD2], COUNT(AVG([FIELD3])) FROM [CATALOG].[SCHEMA].[TABLE] A GROUP BY [FIELD1], [FIELD2], [FIELD3]", rewriter.rewriteSQL(s1));

		} catch (Exception e) {
			fail(e);
		}
	}

}
