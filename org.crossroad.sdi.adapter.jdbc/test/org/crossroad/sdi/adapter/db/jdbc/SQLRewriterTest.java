package org.crossroad.sdi.adapter.db.jdbc;

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
	
	@Test
	public void rewriterWhereAnd() {
		try {
			JDBCSQLRewriter rewriter = new JDBCSQLRewriter();
			String input = "SELECT \"week\".\"week_id\", \"week\".\"year\", \"week\".\"week_cal\", \"week\".\"start_date\", \"week\".\"end_date\" FROM \"<none>.cmdb_rp.week\" \"week\"  WHERE \"week\".\"end_date\" = 1 AND \"week\".\"t\" = 2 LIMIT 1000";
			
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
	  
	@Test
	public void rewriter1() {
		try {
			JDBCSQLRewriter rewriter = new JDBCSQLRewriter();
			String input = "SELECT \"timeslot\".\"status_type\" FROM (\"cmdb_rp.store_workplan\" \"store_workplan\"  LEFT OUTER JOIN \"cmdb_rp.timeslot\" \"timeslot\"  ON (\"store_workplan\".\"date\" = TO_DATE(TO_NVARCHAR(\"timeslot\".\"date\")) AND \"store_workplan\".\"workplan_id\" = \"timeslot\".\"workplan_id\") ) GROUP BY \"timeslot\".\"status_type\" ORDER BY \"timeslot\".\"status_type\" ASC LIMIT 1000";
			
			String sql = rewriter.rewriteSQL(input);
			
			System.out.println(String.format("INPUT [%s]", input));
			System.out.println(String.format("OUTPUT [%s]", sql));
			
			assertNotNull("SQL output must not be null", sql);
			assertEquals("SELECT A.status_type FROM (cmdb_rp.store_workplan B LEFT OUTER JOIN cmdb_rp.timeslot A ON (B.date = CAST(A.date AS DATE) AND B.workplan_id = A.workplan_id)) GROUP BY A.status_type ORDER BY A.status_type ASC LIMIT 1000", sql);
			
		} catch(Exception e)
		{
			fail(e);
		}
	}
	@Test
	public void rewriter2() {

			rewriter( "select\r\n"
					+ "		\"timeslot\".\"date\" as \"V1\",\r\n"
					+ "		\"timeslot\".\"workplan_id\" as \"V2\",\r\n"
					+ "		\"timeslot_status_type\".\"value\" as \"V3\"\r\n"
					+ "	from\r\n"
					+ "		(\"cmdb_rp.timeslot\" \"timeslot\"\r\n"
					+ "	left outer join \"cmdb_rp.timeslot_status_type\" \"timeslot_status_type\" on\r\n"
					+ "		(\"timeslot\".\"status_type\" = \"timeslot_status_type\".\"id\") )");
			
	}
	
	@Test
	public void rewrite3() {
		rewriter("select\r\n"
				+ "	\"store_workplan\".\"week_id\",\r\n"
				+ "	\"timeslot_status_type4\".\"V3\"\r\n"
				+ "from\r\n"
				+ "	( (\r\n"
				+ "	select\r\n"
				+ "		\"timeslot\".\"date\" as \"V1\",\r\n"
				+ "		\"timeslot\".\"workplan_id\" as \"V2\",\r\n"
				+ "		\"timeslot_status_type\".\"value\" as \"V3\"\r\n"
				+ "	from\r\n"
				+ "		(\"cmdb_rp.timeslot\" \"timeslot\"\r\n"
				+ "	left outer join \"cmdb_rp.timeslot_status_type\" \"timeslot_status_type\" on\r\n"
				+ "		(\"timeslot\".\"status_type\" = \"timeslot_status_type\".\"id\") ) ) \"timeslot_status_type4\"\r\n"
				+ "left outer join \"cmdb_rp.store_workplan\" \"store_workplan\" on\r\n"
				+ "	(\"timeslot_status_type4\".\"V2\" = \"store_workplan\".\"workplan_id\"\r\n"
				+ "		and TO_DATE(TO_NVARCHAR(\"timeslot_status_type4\".\"V1\")) = \"store_workplan\".\"date\") )");
	}
	private void rewriter (String input) {
		try {
			JDBCSQLRewriter rewriter = new JDBCSQLRewriter();
			
			String sql = rewriter.rewriteSQL(input);
			
			System.out.println(String.format("INPUT [%s]", input));
			System.out.println(String.format("OUTPUT [%s]", sql));
			
		} catch(Exception e)
		{
			fail(e);
		}
	}
	
	
	@Test
	public void rewriter22() {
		try {
			
			rewriter("SELECT \"store_workplan\".\"week_id\", \"timeslot_status_type4\".\"V3\" FROM ( (SELECT \"timeslot\".\"date\" AS \"V1\", \"timeslot\".\"workplan_id\" AS \"V2\", \"timeslot_status_type\".\"value\" AS \"V3\" FROM (\"cmdb_rp.timeslot\" \"timeslot\"  LEFT OUTER JOIN \"cmdb_rp.timeslot_status_type\" \"timeslot_status_type\"  ON (\"timeslot\".\"status_type\" = \"timeslot_status_type\".\"id\") ) )  \"timeslot_status_type4\"  LEFT OUTER JOIN \"cmdb_rp.store_workplan\" \"store_workplan\"  ON (\"timeslot_status_type4\".\"V2\" = \"store_workplan\".\"workplan_id\" AND TO_DATE(TO_NVARCHAR(\"timeslot_status_type4\".\"V1\")) = \"store_workplan\".\"date\") ) GROUP BY \"store_workplan\".\"week_id\",\"timeslot_status_type4\".\"V3\" ORDER BY \"timeslot_status_type4\".\"V3\" ASC, \"store_workplan\".\"week_id\" ASC LIMIT 200 ");
			
		} catch(Exception e)
		{
			fail(e);
		}
	}
}
