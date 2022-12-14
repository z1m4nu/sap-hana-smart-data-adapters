/**
 * 
 */
package org.crossroad.sdi.adapter.db.singlestore;

import org.crossroad.sdi.adapter.db.mysql.MySQLRewriter;
import org.crossroad.sdi.adapter.impl.functions.AGGREGATE;
import org.crossroad.sdi.adapter.impl.functions.CONVERSION;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;

/**
 * @author e.soden
 *
 */
public class SingleStoreRewriter extends MySQLRewriter {

	@Override
	protected String aggregateFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		AGGREGATE fx = AGGREGATE.valueOf(expr.getValue());

		switch (fx) {
		case VAR:
		case VAR_POP:
		case VAR_SAMP:
			if (AGGREGATE.VAR.equals(fx)) {
				builder.append(super.createFunction("VARIANCE",expr));
			} else {
				builder.append(super.createFunction(expr.getValue(),expr));
			}
			break;
		case STDDEV:
		case STDDEV_POP:
		case STDDEV_SAMP:
			if (AGGREGATE.STDDEV.equals(fx)) {
				builder.append(super.createFunction("STDEV",expr));
				
			} else {
				builder.append(super.createFunction(expr.getValue(),expr));
			}

			break;
		default:
			builder.append(super.aggregateFunctionBuilder(expr));
			break;
		}
		return builder.toString();
	}



	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		StringBuilder builder = new StringBuilder();
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		String value = expressionBuilder(expr.getOperands().get(0));

		if (CONVERSION.TO_REAL.equals(fx) || CONVERSION.TO_DECIMAL.equals(fx) || CONVERSION.TO_DOUBLE.equals(fx)) {
			builder.append("CAST(");
			builder.append(value);
			builder.append(" AS ");

			builder.append("DECIMAL");
			builder.append(")");
			return builder.toString();
		} else {
			return super.castFunctionBuilder(expr);
		}

	}
}
