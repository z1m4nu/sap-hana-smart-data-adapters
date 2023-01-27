package org.crossroad.sdi.adapter.db;

import static org.junit.jupiter.api.Assertions.fail;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.db.mssql.v16x.MSSQLV16xRewriter;
import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.db.postgresql.PGSQLRewriter;
import org.crossroad.sdi.adapter.db.singlestore.SingleStoreRewriter;
import org.junit.Test;

public class SQLTest {
	private JDBCSQLRewriter jdbcRewriter = new JDBCSQLRewriter();
	private MySQLRewriter mySQLrewriter = new MySQLRewriter();
	private MSSQLRewriter msSQLrewriter = new MSSQLRewriter();
	private MSSQLV16xRewriter msSQLv16xrewriter = new MSSQLV16xRewriter();
	private SingleStoreRewriter singleStorerewriter = new SingleStoreRewriter();
	private PGSQLRewriter pgSQLwriter = new PGSQLRewriter();

	String sql1 = "SELECT \"store_workplan\".\"date\", \"store_workplan\".\"week_id\", \"store_workplan\".\"store\", \"store_workplan\".\"approver\", \"store_workplan\".\"confirmed\", \"store_workplan\".\"target\", \"store_workplan\".\"takings\", \"store_workplan\".\"updated_by\", \"store_workplan\".\"created_by\", \"store_workplan\".\"updated\", \"store_workplan\".\"created\", \"timeslot_status_type2\".\"V1\", \"timeslot_status_type2\".\"V2\", \"timeslot_status_type2\".\"V3\", \"timeslot_status_type2\".\"V4\", \"timeslot_status_type2\".\"V5\", \"timeslot_status_type2\".\"V6\", \"timeslot_status_type2\".\"V7\", \"timeslot_status_type2\".\"V8\", \"timeslot_status_type2\".\"V9\", \"timeslot_status_type2\".\"V10\", \"timeslot_status_type2\".\"V11\", \"timeslot_status_type2\".\"V12\" FROM ( (SELECT \"timeslot\".\"barcode_id\" AS \"V1\", \"timeslot\".\"date\" AS \"V2\", \"timeslot\".\"workplan_id\" AS \"V3\", \"timeslot\".\"timeslot_start\" AS \"V4\", \"timeslot\".\"timeslot_end\" AS \"V5\", \"timeslot\".\"status_type\" AS \"V6\", \"timeslot\".\"updated_by\" AS \"V7\", \"timeslot\".\"created_by\" AS \"V8\", \"timeslot\".\"updated\" AS \"V9\", \"timeslot\".\"created\" AS \"V10\", \"timeslot_status_type\".\"value\" AS \"V11\", \"timeslot_status_type\".\"en\" AS \"V12\" FROM (\"cmdb_rp.timeslot\" \"timeslot\"  LEFT OUTER JOIN \"cmdb_rp.timeslot_status_type\" \"timeslot_status_type\"  ON (\"timeslot\".\"status_type\" = \"timeslot_status_type\".\"id\") ) )  \"timeslot_status_type2\"  LEFT OUTER JOIN \"cmdb_rp.store_workplan\" \"store_workplan\"  ON (\"timeslot_status_type2\".\"V3\" = \"store_workplan\".\"workplan_id\" AND TO_DATE(TO_NVARCHAR(\"timeslot_status_type2\".\"V2\")) = \"store_workplan\".\"date\") )  LIMIT 200 " ;
	
	String sqlMysql = "SELECT \"salesrule\".\"row_id\", \"salesrule\".\"sap_promotion_id\", \"salesrule\".\"sap_offer_id\", \"salesrule\".\"sap_promotion_type\", \"salesrule\".\"name\", \"salesrule\".\"from_date\", \"salesrule\".\"to_date\", \"salesrule\".\"is_active\", \"salesrule\".\"coupon_type\" FROM ( (SELECT \"salesrule2\".\"sap_offer_id\" AS \"V1\", MAX(\"salesrule2\".\"row_id\") AS \"V2\" FROM \"tally_db_new.salesrule\" \"salesrule2\" GROUP BY \"salesrule2\".\"sap_offer_id\" )  \"salesrule3\"  INNER JOIN \"tally_db_new.salesrule\" \"salesrule\"  ON (\"salesrule3\".\"V1\" = \"salesrule\".\"sap_offer_id\" AND TO_VARCHAR(\"salesrule3\".\"V2\") = TO_VARCHAR(\"salesrule\".\"row_id\")) ) WHERE \"salesrule\".\"coupon_type\" = 1  LIMIT 200 ";
	@Test
	public void charTest() {
		try {
			System.out.println(String.format("INPUT [%s]", sql1));
			System.out.println(String.format("[PGSQL] OUTPUT [%s]", pgSQLwriter.rewriteSQL(sql1)));
		} catch (Exception e) {
			fail(e);
		}
	}
	
	@Test
	public void mySQL() {
		try {
			System.out.println(String.format("INPUT [%s]", sqlMysql));
			System.out.println(String.format("[MYQL] OUTPUT [%s]", mySQLrewriter.rewriteSQL(sqlMysql)));
		} catch (Exception e) {
			fail(e);
		}
	}


}
