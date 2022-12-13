/**
 * 
 */
package org.crossroad.sdi.adapter.db.mssql;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.functions.AGGREGATE;
import org.crossroad.sdi.adapter.functions.CONVERSION;
import org.crossroad.sdi.adapter.functions.TIME;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
 * @author e.soden
 *
 */
public class MSSQLRewriter extends JDBCSQLRewriter {
	private Logger logger = LogManager.getLogger(MSSQLRewriter.class);

	public MSSQLRewriter() {
		super();
		setLimitAtEnd(false);
	}

	@Override
	protected String expressionCONCAT(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		boolean _first = true;
		for (ExpressionBase param : expr.getOperands()) {
			if (!_first) {
				buffer.append("+");
			}
			buffer.append(expressionBuilder(param));
			_first = false;
		}

		return buffer.toString();
	}

	@Override
	protected String printDT(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();

		buffer.setLength(0);

		String _v = ((Expression) expr.getOperands().get(0)).getValue();

		switch (expr.getType()) {
		case TIMESTAMP_LITERAL:
			// buffer.append("{ts");
			buffer.append("convert(datetime,");
			buffer.append(MSSQLAdapterUtil.buidTS(MSSQLAdapterUtil.str2DT(_v)));
			buffer.append(")");
			// buffer.append("}");
			break;
		default:
			throw new AdapterException("Expression type [" + expr.getType().name() + "] is not supported.");
		}

		return buffer.toString();
	}

	@Override
	protected String tableNameBuilder(TableReference tabRef) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		String tabName = tabRef.getName();
		if (tabName.contains(".")) {
			buffer.append(String.format("[%s]", UniqueNameTools.build(tabName).getUniqueName().replace(".", "].[")));
		} else if (tabRef.getDatabase() != null) {
			buffer.append("[");
			buffer.append(tabRef.getDatabase());
			buffer.append("].");
			buffer.append(tabName);
		} else {
			buffer.append(tabName);
		}
		return buffer.toString();
	}

	@Override
	protected String columnNameBuilder(ColumnReference colRef) {
		StringBuilder buffer = new StringBuilder();

		if (colRef.getTableName() != null) {
			buffer.append(aliasRewriter(colRef.getTableName()) + ".");
		}

		if ("*".equalsIgnoreCase(colRef.getColumnName())) {
			buffer.append(colRef.getColumnName());
		} else {
			buffer.append("[");
			buffer.append(colRef.getColumnName().replaceAll("\"", ""));
			buffer.append("]");
		}
		return buffer.toString();
	}

	
	@Override
	protected String aggregateFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		AGGREGATE fx = AGGREGATE.valueOf(expr.getValue());
		switch (fx) {
		case VAR:
		case VAR_POP:
		case VAR_SAMP:
			if (AGGREGATE.VAR_POP.equals(fx)) {
				builder.append(createFunction("VARP", expr));
			} else {
				builder.append(createFunction(expr.getValue(), expr));
			}
			break;
		case STDDEV:
		case STDDEV_POP:
		case STDDEV_SAMP:
			builder.append(createFunction("STDEV", expr));
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
			builder.append(" DATEADD(");
			if (TIME.ADD_DAYS.equals(fx)) {
				builder.append("day,");
			} else if (TIME.ADD_MONTHS.equals(fx)) {
				builder.append("month,");
			} else if (TIME.ADD_YEARS.equals(fx)) {
				builder.append("year,");
			} else if (TIME.ADD_SECONDS.equals(fx)) {
				builder.append("second,");
			}

			builder.append(expressionBuilder(expr.getOperands().get(1))).append(",")
					.append(expressionBuilder(expr.getOperands().get(0))).append(")");
			break;
		default:
			builder.append(super.timeFunctionBuilder(expr));
			break;
		}

		return builder.toString();

	}

	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		if (CONVERSION.TO_DOUBLE.equals(fx) || CONVERSION.TO_BOOLEAN.equals(fx)) {
			StringBuilder builder = new StringBuilder();

			builder.append("CAST(");
			builder.append(expressionBuilder(expr.getOperands().get(0)));
			builder.append(" AS ");

			if (CONVERSION.TO_DOUBLE.equals(fx)) {
				builder.append(" DOUBLE PRECISION");
			} else if (CONVERSION.TO_BOOLEAN.equals(fx)) {
				builder.append(" BIT");
			}
			builder.append(")");

			return builder.toString();
		} else {
			return super.castFunctionBuilder(expr);
		}
	}

	@Override
	protected String trimFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();

		String trimString = expressionBuilder(expr.getOperands().get(1));
		String value = expressionBuilder(expr.getOperands().get(2));

		return builder.append("TRIM(").append(trimString).append(" FROM ").append(value).append(")").toString();

	}
}
