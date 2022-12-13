/**
 * 
 */
package org.crossroad.sdi.adapter.db.mssql.v16x;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.db.mssql.MSSQLRewriter;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.Expression;

/**
 * @author e.soden
 *
 */
public class MSSQLV16xRewriter extends MSSQLRewriter {
	private Logger logger = LogManager.getLogger(MSSQLV16xRewriter.class);

	
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
}
