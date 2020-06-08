/**
 * 
 */
package org.crossroad.sdi.adapter.db.mssql;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.db.AbstractSQLRewriter;
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
public class SQLRewriter extends AbstractSQLRewriter {
	private Logger logger = LogManager.getLogger(SQLRewriter.class);


	public SQLRewriter() {
		super();
		setLimitAtEnd(false);
	}


	@Override
	protected String expressionCONCAT(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();
		buffer.setLength(0);

		boolean _first = true;
		for (ExpressionBase param : expr.getOperands()) {
			if (!_first)
			{
				buffer.append("+");
			}
			buffer.append(expressionBuilder(param));
			_first = false;
		}
		
		return buffer.toString();
	}


	@Override
	protected String printDT(Expression expr) throws AdapterException {
		StringBuffer buffer = new StringBuffer();

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
		StringBuffer buffer = new StringBuffer();
		String tabName = tabRef.getName();
		if (tabName.contains(".")) {
			buffer.append(MSSQLAdapterUtil.SQLTableBuilder(UniqueNameTools.build(tabName)));
			// buffer.append(UniqueNameTools.build(tabName).getUniqueName());
		} else if (tabRef.getDatabase() != null) {
			buffer.append("[");
			buffer.append(tabRef.getDatabase());
			buffer.append("].");
		}
		return buffer.toString();
	}

	@Override
	protected String columnNameBuilder(ColumnReference colRef) {
		StringBuffer buffer = new StringBuffer();
		
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

}
