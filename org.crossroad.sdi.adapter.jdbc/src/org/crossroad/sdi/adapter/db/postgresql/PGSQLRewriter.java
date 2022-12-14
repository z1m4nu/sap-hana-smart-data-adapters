/**
 * 
 */
package org.crossroad.sdi.adapter.db.postgresql;

import org.crossroad.sdi.adapter.db.jdbc.JDBCSQLRewriter;
import org.crossroad.sdi.adapter.impl.functions.CONVERSION;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;

/**
 * @author e.soden
 *
 */
public class PGSQLRewriter extends JDBCSQLRewriter {


	
	@Override
	protected String castFunctionBuilder(Expression expr) throws AdapterException {
		CONVERSION fx = CONVERSION.valueOf(expr.getValue());
		if (CONVERSION.TO_TINYINT.equals(fx) || CONVERSION.TO_DOUBLE.equals(fx)) {
			StringBuilder builder = new StringBuilder();

			builder.append("CAST(");
			builder.append(expressionBuilder(expr.getOperands().get(0)));
			builder.append(" AS ");

			if (CONVERSION.TO_TINYINT.equals(fx)) {
				builder.append(" SMALLINT");
			} else if (CONVERSION.TO_DOUBLE.equals(fx)) {
				builder.append(" DOUBLE PRECISION");
			}
			builder.append(")");

			return builder.toString();
		} else {
			return super.castFunctionBuilder(expr);
		}
	}

}
