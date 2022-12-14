/**
 * 
 */
package org.crossroad.sdi.adapter.db.jdbc;

import java.util.List;

import org.crossroad.sdi.adapter.db.AbstractSQLRewriter;
import org.crossroad.sdi.adapter.impl.functions.AGGREGATE;
import org.crossroad.sdi.adapter.impl.functions.CONVERSION;
import org.crossroad.sdi.adapter.impl.functions.MISC;
import org.crossroad.sdi.adapter.impl.functions.NUMERIC;
import org.crossroad.sdi.adapter.impl.functions.STRING;
import org.crossroad.sdi.adapter.impl.functions.TIME;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;

/**
 * @author e.soden
 *
 */
public class JDBCSQLRewriter extends AbstractSQLRewriter {

	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		builder.append("CAST(");
		builder.append(expressionBuilder(expr.getOperands().get(0)));
		builder.append(" AS ");
		

		if (CONVERSION.TO_DOUBLE.equals(fx)) {
			builder.append("DECIMAL");
		} else if (CONVERSION.TO_DECIMAL.equals(fx)) {
			List<ExpressionBase> params = expr.getOperands();
			if (params.size() < 3) {
				builder.append("CAST(");
				builder.append(expressionBuilder(expr.getOperands().get(0)));
				builder.append(" AS DECIMAL");
				builder.append(")");
			} else {
				builder.append("CAST(");
				builder.append(expressionBuilder(expr.getOperands().get(0)));
				builder.append(" AS DECIMAL(");
				builder.append(expressionBuilder(expr.getOperands().get(1)));
				builder.append(",");
				builder.append(expressionBuilder(expr.getOperands().get(2)));
				builder.append(")");
				builder.append(")");
			}
		} else {
			builder.append(fx.name().substring(fx.name().indexOf('_')+1));
		}
		builder.append(")");

		return builder.toString();
	}

	@Override
	protected String stringFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		STRING fx = STRING.valueOf(expr.getValue());
		switch (fx) {
		case CONCAT:
			builder.append(expressionCONCAT(expr));
			break;
		case LOCATE:
		case LPAD:
		case REPLACE:
		case RPAD:
		case SUBSTRING:
			builder.append(fx.name()).append("(");

			builder.append(expressionBuilder(expr.getOperands().get(0)))
					.append(expressionBuilder(expr.getOperands().get(1)));

			if (expr.getOperands().size() == 3) {
				builder.append(expressionBuilder(expr.getOperands().get(2)));
			}
			builder.append(")");
			break;
		case TRIM:
			builder.append(trimFunctionBuilder(expr));
			break;

		case CHAR:
		case LCASE:
		case LEFT:
		case LENGTH:
		case LOWER:
		case LTRIM:
		case RIGHT:
		case RTRIM:
		case SOUNDEX:
		case UPPER:
			builder.append(createFunction(fx.name(), expr));
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

			builder.append(String.format("%s + INTERVAL '%d %s'", left, Integer.valueOf(right), intervalType));
			break;
		case CURRENT_DATE:
		case CURRENT_TIME:
		case CURRENT_TIMESTAMP:
			builder.append(fx.name());
			break;
		}

		return builder.toString();
	}

	@Override
	protected String trimFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		builder.append("TRIM(");

		String direction = expressionBuilder(expr.getOperands().get(0));
		String trimString = expressionBuilder(expr.getOperands().get(1));
		String value = expressionBuilder(expr.getOperands().get(2));

		if (StringUtils.hasText(direction)) {
			builder.append(direction).append(" ");
		}

		return builder.append(trimString).append(" FROM ").append(value).append(")").toString();

	}

	@Override
	protected String miscFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		MISC fx = MISC.valueOf(expr.getValue());
		switch (fx) {
		case IFNULL:
			builder.append(createFunction(MISC.COALESCE.name(), expr));
			break;
		case LEAST:
		case GREATEST:
		case COALESCE:
			builder.append(createFunction(fx.name(), expr));
			break;
		}
		return builder.toString();
	}

	/**
	 * 
	 * @param keyword
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String createFunction(String keyword, Expression expr) throws AdapterException {
		boolean first = true;
		StringBuilder buffer = new StringBuilder();
		try {

			buffer.append(keyword);
			buffer.append("(");

			
			for (ExpressionBase param : expr.getOperands()) {
				if (first) {
					if (param.getType() == ExpressionBase.Type.DISTINCT) {
						buffer.append("DISTINCT ");
						continue;
					}
					first = false;
				} else {
					buffer.append(", ");
				}
				buffer.append(expressionBuilder(param));
			}
			
			return buffer.append(")").toString();
		} catch (Exception e) {
			throw new AdapterException(e);
		}
	}

	@Override
	protected String numericFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		NUMERIC fx = NUMERIC.valueOf(expr.getValue());

		switch (fx) {
		case ABS:
		case ADD:
		case ACOS:
		case CEIL:
		case COS:
		case COT:
		case EXP:
		case FLOOR:
		case LN:
		case LOG:
		case MOD:
		case ROUND:
			builder.append(createFunction(fx.name(), expr));
			break;
		case ATAN2:
			builder.append(createFunction("ATN2", expr));
			break;
		
		}
		return builder.toString();
	}

	@Override
	protected String aggregateFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		AGGREGATE fx = AGGREGATE.valueOf(expr.getValue());

		switch (fx) {
		case AVG:
		case COUNT:
		case MAX:
		case MIN:
		case SUM:
		case STDDEV:
		case STDDEV_POP:
		case STDDEV_SAMP:
			builder.append(createFunction(fx.name(), expr));
			break;
		
		case VAR:
		case VAR_POP:
		case VAR_SAMP:
			if (AGGREGATE.VAR.equals(fx)) {
				builder.append(createFunction("VARIANCE (",expr));
			} else  {
				builder.append(createFunction(fx.name(), expr));
			}

			break;
		}

		return builder.toString();

	}

}
