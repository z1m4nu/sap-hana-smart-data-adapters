/**
 * 
 */
package org.crossroad.sdi.adapter.db.postgresql;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.impl.functions.CONVERSION;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;

/**
 * @author e.soden
 *
 */
public class PGSQLRewriter extends JDBCSQLRewriter {
	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		StringBuilder builder = new StringBuilder();

		switch (fx) {
		
		case TO_TINYINT:
		case TO_DOUBLE:
			builder.append("CAST(");
			builder.append(expressionBuilder(expr.getOperands().get(0)));
			builder.append(" AS ");

			if (CONVERSION.TO_TINYINT.equals(fx)) {
				builder.append(" SMALLINT");
			} else if (CONVERSION.TO_DOUBLE.equals(fx)) {
				builder.append(" DOUBLE PRECISION");
			}
			builder.append(")");
			break;

		case TO_NVARCHAR:
			builder.append("CAST(");
			builder.append(expressionBuilder(expr.getOperands().get(0)));
			builder.append(" AS VARCHAR)");
			break;
		default:
			builder.append(super.castFunctionBuilder(expr));
			break;
		}

		return builder.toString();

	}

}
