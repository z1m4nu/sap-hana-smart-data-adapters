/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import com.sap.hana.dp.adapter.sdk.Capabilities;

import java.util.ArrayList;
import java.util.List;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.AdapterCapability;

/**
 * @author e.soden
 *
 */
public final class CapabilitiesUtils {

	/**
	 * 
	 */
	private CapabilitiesUtils() {
	}

	public static List<AdapterCapability> getDMLModifier() {
		List<AdapterCapability> list = new ArrayList<AdapterCapability>();

		list.add(AdapterCapability.CAP_INSERT);
		list.add(AdapterCapability.CAP_INSERT_SELECT);
		list.add(AdapterCapability.CAP_INSERT_SELECT_ORDERBY);

		list.add(AdapterCapability.CAP_UPDATE);
		list.add(AdapterCapability.CAP_SUBQUERY_SELLIST);
		list.add(AdapterCapability.CAP_SUBQUERY_UPDATE);

		list.add(AdapterCapability.CAP_DELETE);

		return list;
	}

	public static List<AdapterCapability> getDDLCapabilities() {
		List<AdapterCapability> list = new ArrayList<AdapterCapability>();

		list.add(AdapterCapability.CAP_ALTER_TAB_WITH_ADD);
		list.add(AdapterCapability.CAP_ALTER_TAB_WITH_DROP);

		list.add(AdapterCapability.CAP_CREATE_INDEX);
		list.add(AdapterCapability.CAP_CREATE_UNIQUE_INDEX);

		list.add(AdapterCapability.CAP_CRT_TABLE_CONSTRAINTS);
		list.add(AdapterCapability.CAP_CRT_TEMP_TABLES);

		return list;
	}

	/**
	 * 
	 * @return
	 */
	public static List<AdapterCapability> getSelectCapabilities() {
		List<AdapterCapability> capabilities = new ArrayList<AdapterCapability>();
		capabilities.add(AdapterCapability.CAP_SELECT);
		capabilities.add(AdapterCapability.CAP_WHERE);
		capabilities.add(AdapterCapability.CAP_GROUPBY);
		capabilities.add(AdapterCapability.CAP_ORDERBY);
		capabilities.add(AdapterCapability.CAP_HAVING);

		capabilities.add(AdapterCapability.CAP_LIMIT);
		capabilities.add(AdapterCapability.CAP_LIMIT_ARG);

		capabilities.add(AdapterCapability.CAP_AND);
		capabilities.add(AdapterCapability.CAP_AND_DIFFERENT_COLUMNS);

		capabilities.add(AdapterCapability.CAP_OR);
		capabilities.add(AdapterCapability.CAP_OR_DIFFERENT_COLUMNS);

		capabilities.add(AdapterCapability.CAP_CASE_EXPRESSION);
		capabilities.add(AdapterCapability.CAP_PROJECT);

		capabilities.add(AdapterCapability.CAP_WINDOWING_FUNC);
		capabilities.add(AdapterCapability.CAP_BIGINT_BIND);

		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_PROJ);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_SIMPLE_EXPR_IN_ORDERBY);

		capabilities.add(AdapterCapability.CAP_EXPR_IN_PROJ);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_EXPR_IN_ORDERBY);

		capabilities.add(AdapterCapability.CAP_INTERSECT);
		capabilities.add(AdapterCapability.CAP_INTERSECTALL);

		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_FULL_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_LEFT_OUTER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_INNER_JOIN);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_WHERE);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_GROUPBY);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_ORDERBY);
		capabilities.add(AdapterCapability.CAP_NESTED_FUNC_IN_PROJ);

		capabilities.add(AdapterCapability.CAP_SCALAR_FUNCTIONS_NEED_ARGUMENT_CHECK);
		capabilities.add(AdapterCapability.CAP_NONEQUAL_COMPARISON);

		capabilities.add(AdapterCapability.CAP_LIKE);

		capabilities.add(AdapterCapability.CAP_AGGREGATES);
		capabilities.add(AdapterCapability.CAP_AGGREGATE_COLNAME);
		capabilities.add(AdapterCapability.CAP_AGGREGATES_LIST);
		capabilities.add(AdapterCapability.CAP_AGGR_VAR);

		capabilities.add(AdapterCapability.CAP_JOINS);
		capabilities.add(AdapterCapability.CAP_JOINS_OUTER);
		capabilities.add(AdapterCapability.CAP_JOINS_FULL_OUTER);
		capabilities.add(AdapterCapability.CAP_JOINS_MIXED);

		capabilities.add(AdapterCapability.CAP_BETWEEN);
		capabilities.add(AdapterCapability.CAP_IN);
		capabilities.add(AdapterCapability.CAP_CONTAINS);

		capabilities.add(AdapterCapability.CAP_SUBQUERY);
		capabilities.add(AdapterCapability.CAP_SUBQUERY_CORRELATED);
		capabilities.add(AdapterCapability.CAP_SUBQUERY_GROUPBY);
		capabilities.add(AdapterCapability.CAP_SUBQUERY_IN_COMPARISON);
		capabilities.add(AdapterCapability.CAP_SUBQUERY_IN_EXIST);
		capabilities.add(AdapterCapability.CAP_SUBQUERY_IN_IN);

		capabilities.add(AdapterCapability.CAP_UNION);
		capabilities.add(AdapterCapability.CAP_UNIONALL);

		capabilities.add(AdapterCapability.CAP_TEXTDATA);

		capabilities.add(AdapterCapability.CAP_SEQUENCE_EXPRESSION);

		return capabilities;
	}

	/**
	 * 
	 * @return
	 */
	public static List<AdapterCapability> getBICapabilities() {
		List<AdapterCapability> list = new ArrayList<AdapterCapability>();
		/*
		 * Date & time functions
		 */
		list.add(AdapterCapability.CAP_BI_TODAY);
		list.add(AdapterCapability.CAP_BI_DAYS);
		list.add(AdapterCapability.CAP_BI_SECOND);
		list.add(AdapterCapability.CAP_BI_MINUTE);
		list.add(AdapterCapability.CAP_BI_HOUR);
		list.add(AdapterCapability.CAP_BI_DAY);
		list.add(AdapterCapability.CAP_BI_MONTH);
		list.add(AdapterCapability.CAP_BI_HOURS);
		list.add(AdapterCapability.CAP_BI_MINUTES);
		list.add(AdapterCapability.CAP_BI_SECONDS);
		list.add(AdapterCapability.CAP_BI_NOW);
		list.add(AdapterCapability.CAP_BI_WEEKS);
		list.add(AdapterCapability.CAP_BI_DATETIME);
		list.add(AdapterCapability.CAP_BI_DATE);
		list.add(AdapterCapability.CAP_BI_DAYNAME);
		list.add(AdapterCapability.CAP_BI_MONTHNAME);
		list.add(AdapterCapability.CAP_BI_QUARTER);
		list.add(AdapterCapability.CAP_BI_DATEPART);
		list.add(AdapterCapability.CAP_BI_DATEDIFF);
		list.add(AdapterCapability.CAP_BI_DATENAME);
		list.add(AdapterCapability.CAP_BI_DATEADD);
		list.add(AdapterCapability.CAP_BI_GETDATE);
		list.add(AdapterCapability.CAP_BI_YMD);
		list.add(AdapterCapability.CAP_BI_TODATE);
		list.add(AdapterCapability.CAP_BI_MONTHS);
		list.add(AdapterCapability.CAP_BI_YEAR);
		list.add(AdapterCapability.CAP_BI_YEARS);
		list.add(AdapterCapability.CAP_BI_DATEFORMAT);
		list.add(AdapterCapability.CAP_BI_DATEFLOOR);
		list.add(AdapterCapability.CAP_BI_DATECEILING);
		list.add(AdapterCapability.CAP_BI_DATEROUND);

		/*
		 * MNaths functions
		 */
		list.add(AdapterCapability.CAP_BI_MOD);
		list.add(AdapterCapability.CAP_BI_ATN2);
		list.add(AdapterCapability.CAP_BI_DEGREES);
		list.add(AdapterCapability.CAP_BI_EXP);
		list.add(AdapterCapability.CAP_BI_FLOOR);
		list.add(AdapterCapability.CAP_BI_LOG);
		list.add(AdapterCapability.CAP_BI_LOG10);
		list.add(AdapterCapability.CAP_BI_PI);
		list.add(AdapterCapability.CAP_BI_POWER);
		list.add(AdapterCapability.CAP_BI_RADIANS);
		list.add(AdapterCapability.CAP_BI_RAND);
		list.add(AdapterCapability.CAP_BI_ROUND);
		list.add(AdapterCapability.CAP_BI_SIGN);
		list.add(AdapterCapability.CAP_BI_SIN);
		list.add(AdapterCapability.CAP_BI_SQRT);
		list.add(AdapterCapability.CAP_BI_TAN);
		list.add(AdapterCapability.CAP_BI_ABS);
		list.add(AdapterCapability.CAP_BI_ACOS);
		list.add(AdapterCapability.CAP_BI_ASIN);
		list.add(AdapterCapability.CAP_BI_ATAN);
		list.add(AdapterCapability.CAP_BI_ATAN2);
		list.add(AdapterCapability.CAP_BI_CEILING);
		list.add(AdapterCapability.CAP_BI_COS);
		list.add(AdapterCapability.CAP_BI_CEIL);
		list.add(AdapterCapability.CAP_BI_SQUARE);
		list.add(AdapterCapability.CAP_BI_ADD);
		list.add(AdapterCapability.CAP_BI_SUB);
		list.add(AdapterCapability.CAP_BI_MUL);
		list.add(AdapterCapability.CAP_BI_DIV);
		list.add(AdapterCapability.CAP_BI_COSH);
		list.add(AdapterCapability.CAP_BI_SINH);
		list.add(AdapterCapability.CAP_BI_TANH);

		/*
		 * String functions
		 */
		list.add(AdapterCapability.CAP_BI_SUBSTR);
		list.add(AdapterCapability.CAP_BI_SUBSTRING);
		list.add(AdapterCapability.CAP_BI_CHAR);
		list.add(AdapterCapability.CAP_BI_TRUNCATE);
		list.add(AdapterCapability.CAP_BI_LCASE);
		list.add(AdapterCapability.CAP_BI_UCASE);
		list.add(AdapterCapability.CAP_BI_LENGTH);
		list.add(AdapterCapability.CAP_BI_TRIM);
		list.add(AdapterCapability.CAP_BI_ASCII);
		list.add(AdapterCapability.CAP_BI_ASCIICHAR);
		list.add(AdapterCapability.CAP_BI_CHARINDEX);
		list.add(AdapterCapability.CAP_BI_CHARLEN);
		list.add(AdapterCapability.CAP_BI_LOWER);
		list.add(AdapterCapability.CAP_BI_LTRIM);
		list.add(AdapterCapability.CAP_BI_REPLICATE);
		list.add(AdapterCapability.CAP_BI_REVERSE);
		list.add(AdapterCapability.CAP_BI_RIGHT);
		list.add(AdapterCapability.CAP_BI_RTRIM);
		list.add(AdapterCapability.CAP_BI_LEN);
		list.add(AdapterCapability.CAP_BI_STR_REPLACE);
		list.add(AdapterCapability.CAP_BI_LPAD);
		list.add(AdapterCapability.CAP_BI_NCHAR);
		list.add(AdapterCapability.CAP_BI_RPAD);
		list.add(AdapterCapability.CAP_BI_LEFT);
		list.add(AdapterCapability.CAP_BI_STR);
		list.add(AdapterCapability.CAP_BI_UPPER);
		list.add(AdapterCapability.CAP_BI_CONCAT);

		/*
		 * Convertion functions
		 */
		list.add(AdapterCapability.CAP_BI_CAST);
		list.add(AdapterCapability.CAP_BI_TO_DECIMAL);
		list.add(AdapterCapability.CAP_BI_TO_TINYINT);
		list.add(AdapterCapability.CAP_BI_TO_DOUBLE);
		list.add(AdapterCapability.CAP_BI_TO_BIGINT);
		list.add(AdapterCapability.CAP_BI_TO_REAL);
		list.add(AdapterCapability.CAP_BI_TO_ALPHANUM);
		list.add(AdapterCapability.CAP_BI_TO_BINARY);
		list.add(AdapterCapability.CAP_BI_TO_NVARCHAR);
		list.add(AdapterCapability.CAP_BI_TO_SMALLDECIMAL);
		list.add(AdapterCapability.CAP_BI_TO_BLOB);
		list.add(AdapterCapability.CAP_BI_TO_CLOB);
		list.add(AdapterCapability.CAP_BI_TO_NCLOB);
		list.add(AdapterCapability.CAP_BI_TO_DATS);
		list.add(AdapterCapability.CAP_BI_TO_TIME);
		list.add(AdapterCapability.CAP_BI_TO_TIMESTAMP);
		list.add(AdapterCapability.CAP_BI_TO_SECONDDATE);
		list.add(AdapterCapability.CAP_BI_TO_VARCHAR);
		list.add(AdapterCapability.CAP_BI_TO_INT);
		list.add(AdapterCapability.CAP_BI_TO_INTEGER);
		list.add(AdapterCapability.CAP_BI_TO_SMALLINT);
		list.add(AdapterCapability.CAP_BI_CONVERT);
		list.add(AdapterCapability.CAP_BI_NUMBER);

		/*
		 * DATA Test
		 */
		list.add(AdapterCapability.CAP_BI_IFNULL);
		list.add(AdapterCapability.CAP_BI_ISNULL);
		list.add(AdapterCapability.CAP_BI_ISDATE);

		// list.add(AdapterCapability.CAP_BIGINT_BIND);
		// list.add(AdapterCapability.CAP_BI_ARGN);
		// list.add(AdapterCapability.CAP_BI_IDENTITY);
		// list.add(AdapterCapability.CAP_BI_REMAINDER);
		// list.add(AdapterCapability.CAP_BI_EXPLANATION);
		// list.add(AdapterCapability.CAP_BI_PLAN);
		// list.add(AdapterCapability.CAP_BI_ULPLAN);
		// list.add(AdapterCapability.CAP_BI_TRACEBACK);
		// list.add(AdapterCapability.CAP_BI_ESTIMATE);
		// list.add(AdapterCapability.CAP_BI_ESTIMATE_SOURCE);
		// list.add(AdapterCapability.CAP_BI_INDEX_ESTIMATE);
		// list.add(AdapterCapability.CAP_BI_EXPERIENCE_ESTIMATE);
		// list.add(AdapterCapability.CAP_BI_TSEQUAL);
		// list.add(AdapterCapability.CAP_BI_DATALENGTH);
		// list.add(AdapterCapability.CAP_BI_DB_ID);
		// list.add(AdapterCapability.CAP_BI_DB_NAME);
		// list.add(AdapterCapability.CAP_BI_PROPERTY_NAME);
		// list.add(AdapterCapability.CAP_BI_PROPERTY_DESCRIPTION);
		// list.add(AdapterCapability.CAP_BI_PROPERTY_NUMBER);
		// list.add(AdapterCapability.CAP_BI_NEXT_CONNECTION);
		// list.add(AdapterCapability.CAP_BI_NEXT_DATABASE);
		// list.add(AdapterCapability.CAP_BI_PROPERTY);
		// list.add(AdapterCapability.CAP_BI_CONNECTION_PROPERTY);
		// list.add(AdapterCapability.CAP_BI_DB_PROPERTY);
		// list.add(AdapterCapability.CAP_BI_TEXTPTR);
		// list.add(AdapterCapability.CAP_BI_ROWID);
		// list.add(AdapterCapability.CAP_BI_USER_ID);
		// list.add(AdapterCapability.CAP_BI_USER_NAME);
		// list.add(AdapterCapability.CAP_BI_SUSER_ID);
		// list.add(AdapterCapability.CAP_BI_SUSER_NAME);
		// list.add(AdapterCapability.CAP_BI_COT);
		// list.add(AdapterCapability.CAP_BI_HEXTOINT);
		// list.add(AdapterCapability.CAP_BI_INTTOHEX);

		// list.add(AdapterCapability.CAP_BI_PATTERN);
		// list.add(AdapterCapability.CAP_BI_BYTE_LENGTH);
		// list.add(AdapterCapability.CAP_BI_BYTE_SUBSTR);
		// list.add(AdapterCapability.CAP_BI_INSERTSTR);

		// list.add(AdapterCapability.CAP_BI_LOCATE);
		// list.add(AdapterCapability.CAP_BI_REPEAT);
		// list.add(AdapterCapability.CAP_BI_SIMILAR);
		// list.add(AdapterCapability.CAP_BI_DIFFERENCE);
		// list.add(AdapterCapability.CAP_BI_PATINDEX);
		// list.add(AdapterCapability.CAP_BI_SOUNDEX);
		// list.add(AdapterCapability.CAP_BI_SPACE);
		// list.add(AdapterCapability.CAP_BI_STUFF);
		// list.add(AdapterCapability.CAP_BI_COALESCE);
		// list.add(AdapterCapability.CAP_BI_STRING);
		// list.add(AdapterCapability.CAP_BI_DOW);
		// list.add(AdapterCapability.CAP_BI_NULLIF);
		// list.add(AdapterCapability.CAP_BI_CHAR_LENGTH);
		// list.add(AdapterCapability.CAP_BI_REPLACE);
		// list.add(AdapterCapability.CAP_BI_TRUNCNUM);
		// list.add(AdapterCapability.CAP_BI_RANK);
		// list.add(AdapterCapability.CAP_BI_DENSE_RANK);
		// list.add(AdapterCapability.CAP_BI_PERCENT_RANK);
		// list.add(AdapterCapability.CAP_BI_SORTKEY);
		// list.add(AdapterCapability.CAP_BI_CUME_DISC_OBSOLETE);
		// list.add(AdapterCapability.CAP_BI_PERCENTILE_CONT);
		// list.add(AdapterCapability.CAP_BI_PERCENTILE_DISC);
		// list.add(AdapterCapability.CAP_BI_NTILE);
		// list.add(AdapterCapability.CAP_BI_BYTE_LENGTH64);
		// list.add(AdapterCapability.CAP_BI_BYTE_SUBSTR64);
		// list.add(AdapterCapability.CAP_BI_BFILE);
		// list.add(AdapterCapability.CAP_BI_BIT_LENGTH);
		// list.add(AdapterCapability.CAP_BI_OCTET_LENGTH);
		// list.add(AdapterCapability.CAP_BI_CHAR_LENGTH64);
		// list.add(AdapterCapability.CAP_BI_SUBSTRING64);
		// list.add(AdapterCapability.CAP_BI_GROUP_MEMBER);
		// list.add(AdapterCapability.CAP_BI_HEXTOBIGINT);
		// list.add(AdapterCapability.CAP_BI_BIGINTTOHEX);
		// list.add(AdapterCapability.CAP_BI_AES_ENCRYPT);
		// list.add(AdapterCapability.CAP_BI_AES_DECRYPT);
		// list.add(AdapterCapability.CAP_BI_WIDTH_BUCKET);
		// list.add(AdapterCapability.CAP_BI_LN);
		// list.add(AdapterCapability.CAP_BI_NEWID);
		// list.add(AdapterCapability.CAP_BI_UUIDTOSTR);
		// list.add(AdapterCapability.CAP_BI_STRTOUUID);
		// list.add(AdapterCapability.CAP_BI_HTML_PLAN);
		// list.add(AdapterCapability.CAP_BI_MEDIAN);
		// list.add(AdapterCapability.CAP_BI_WEIGHTED_AVG);
		// list.add(AdapterCapability.CAP_BI_EXP_WEIGHTED_AVG);
		// list.add(AdapterCapability.CAP_BI_ROW_NUMBER);
		// list.add(AdapterCapability.CAP_BI_LEAD);
		// list.add(AdapterCapability.CAP_BI_LAG);
		// list.add(AdapterCapability.CAP_BI_BITAND);
		// list.add(AdapterCapability.CAP_BI_UNICODE);

		// list.add(AdapterCapability.CAP_BINARY_TRANSFER);

		return list;
	}

}
