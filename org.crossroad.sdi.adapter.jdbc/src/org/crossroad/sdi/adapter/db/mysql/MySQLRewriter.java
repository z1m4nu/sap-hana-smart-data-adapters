/**
 * 
 */
package org.crossroad.sdi.adapter.db.mysql;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.impl.functions.AGGREGATE;
import org.crossroad.sdi.adapter.impl.functions.CONVERSION;
import org.crossroad.sdi.adapter.impl.functions.TIME;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;

/**
 * @author e.soden
 *
 */
public class MySQLRewriter extends JDBCSQLRewriter {

	@Override
	protected String aggregateFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		AGGREGATE fx = AGGREGATE.valueOf(expr.getValue());
		switch (fx) {
		case VAR:
		case VAR_POP:
		case VAR_SAMP:
			if (AGGREGATE.VAR.equals(fx)) {
				builder.append(createFunction("VARIANCE", expr));
			} else if (AGGREGATE.VAR_POP.equals(fx)) {
				builder.append(createFunction(expr.getValue(), expr));
			} else if (AGGREGATE.VAR_SAMP.equals(fx)) {
				builder.append(createFunction("VARP_SAM", expr));
			}
			break;
		case STDDEV:
		case STDDEV_POP:
		case STDDEV_SAMP:
			if (AGGREGATE.STDDEV.equals(fx)) {
				builder.append(createFunction("STDEV", expr));
			} else {
				builder.append(createFunction(expr.getValue(), expr));
			}
			break;
		default:
			builder.append(super.aggregateFunctionBuilder(expr));
			break;
		}

		return builder.toString();
	}

	@Override
	protected String timeFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		TIME fx = TIME.valueOf(expr.getValue());
		switch (fx) {
		case ADD_DAYS:
		case ADD_MONTHS:
		case ADD_YEARS:
		case ADD_SECONDS:
			String intervalType = null;

			if (TIME.ADD_DAYS.equals(fx)) {
				intervalType = "DAY";
			} else if (TIME.ADD_MONTHS.equals(fx)) {
				intervalType = "MONTH";
			} else if (TIME.ADD_YEARS.equals(fx)) {
				intervalType = "YEAR";
			} else if (TIME.ADD_SECONDS.equals(fx)) {
				intervalType = "SECOND";
			}

			String left = expressionBuilder(expr.getOperands().get(0));
			String right = expressionBuilder(expr.getOperands().get(1));

			builder.append(String.format(" %s(%s, INTERVAL %d %s) ", (right.startsWith("-") ? "DATE_SUB" : "DATE_ADD"),
					left, Math.abs(Integer.valueOf(right)), intervalType));
		default:
			builder.append(super.timeFunctionBuilder(expr));
			break;
		}
		return builder.toString();
	}

	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		String value = expressionBuilder(expr.getOperands().get(0));

		switch (fx) {

		case TO_BOOLEAN:
			builder.append("CAST(");
			if (value.toLowerCase().contains("true") || value.toLowerCase().contains("false")) {
				builder.append("(").append(value.toLowerCase()).append(" = 'true')");
				builder.append(" AS SIGNED");
			} else if (StringUtils.isNumeric(value)) {
				builder.append(value).append(" AS SIGNED");
			}
			builder.append(")");
			break;
		case TO_DOUBLE:
			builder.append("CAST(");
			builder.append(value);
			builder.append(" AS DOUBLE PRECISION)");
			break;
		case TO_BIGINT:
		case TO_INT:
		case TO_INTEGER:
		case TO_TINYINT:
		case TO_SMALLINT:
			builder.append("CAST(");
			builder.append(value);
			builder.append(" AS  SIGNED INTEGER)");
			break;
		case TO_VARCHAR:
			builder.append("CAST(");
			builder.append(value);
			builder.append(" AS ");
			builder.append("CHAR)");
			break;
		case TO_NVARCHAR:
			builder.append("CAST(");
			builder.append(value);
			builder.append(" AS NCHAR)");
			break;
		default:
			builder.append(super.castFunctionBuilder(expr));
			break;
		}

		return builder.toString();
	}
}
