package org.crossroad.sdi.adapter.db.pgsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.junit.Before;
import org.junit.Test;

public class SQLRewriterTest {
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void rewriter() {
		try {
			JDBCSQLRewriter rewriter = new JDBCSQLRewriter();
			String input = "SELECT \"week\".\"week_id\", \"week\".\"year\", \"week\".\"week_cal\", \"week\".\"start_date\", \"week\".\"end_date\" FROM \"<none>.cmdb_rp.week\" \"week\"  LIMIT 1000";
			
			String sql = rewriter.rewriteSQL(input);
			
			System.out.println(String.format("INPUT [%s]", input));
			System.out.println(String.format("OUTPUT [%s]", sql));
			
			assertNotNull("SQL output must not be null", sql);
			assertEquals("SELECT A.week_id, A.year, A.week_cal, A.start_date, A.end_date FROM cmdb_rp.week A LIMIT 1000", sql);
		
			
		} catch(Exception e)
		{
			fail(e);
		}
	}
	
}
