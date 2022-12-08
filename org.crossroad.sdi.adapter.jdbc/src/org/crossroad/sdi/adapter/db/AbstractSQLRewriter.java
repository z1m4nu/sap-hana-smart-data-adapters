/**
 * 
 */
package org.crossroad.sdi.adapter.db;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.impl.FUNCTIONS;
import org.crossroad.sdi.adapter.impl.ISQLRewriter;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.parser.ColumnReference;
import com.sap.hana.dp.adapter.sdk.parser.Expression;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionBase.Type;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserMessage;
import com.sap.hana.dp.adapter.sdk.parser.ExpressionParserUtil;
import com.sap.hana.dp.adapter.sdk.parser.Join;
import com.sap.hana.dp.adapter.sdk.parser.Order;
import com.sap.hana.dp.adapter.sdk.parser.Query;
import com.sap.hana.dp.adapter.sdk.parser.TableReference;

/**
 * @author e.soden
 *
 */
public abstract class AbstractSQLRewriter implements ISQLRewriter {
	private Logger logger = LogManager.getLogger(AbstractSQLRewriter.class);
	
	

	private char aliasSeed = 'A';

	private Map<String, String> aliasMap = new HashMap<String, String>();
	private int maxIdentifierLength;
	private ExpressionBase.Type queryType = ExpressionBase.Type.QUERY;
	private final Map<String, String> schemaAliasReplacements = new HashMap<String, String>();
	private boolean limitAtEnd = true;

	public void setLimitAtEnd(boolean limitAtEnd) {
		this.limitAtEnd = limitAtEnd;
	}

	@Override
	public Type getQueryType() {
		return this.queryType;
	}

	public void setQueryType(ExpressionBase.Type type) {
		this.queryType = type;
	}

	@Override
	public void setMaxIndentifierLength(int maxIdentifierLength) {
		this.maxIdentifierLength = maxIdentifierLength;
	}

	@Override
	public void addSchemaAliasReplacement(String schemaAlias, String schemaAliasReplacement) {
		if (schemaAlias != null) {
			this.schemaAliasReplacements.put(schemaAlias, schemaAliasReplacement);
		}
	}

	protected int getMaxIdentifierLength() {
		return maxIdentifierLength;
	}

	protected String printFxColumns(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);
		boolean first = true;

		try {
			FUNCTIONS fx = FUNCTIONS.valueOf(expr.getValue());

			buffer.append(fx.name());
			buffer.append("(");

			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append(", ");
				}

				buffer.append(expressionBuilder(b));

			}
			buffer.append(")");
		} catch (Exception e) {
			throw new AdapterException(e);
		}

		return buffer.toString();
	}

	protected String clauseBuilder(String keyword, List<ExpressionBase> eprx) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append(keyword);
		buffer.append(expressionBaseListBuilder(eprx));

		return buffer.toString();
	}

	/**
	 * Build the FROM clause
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String fromClauseBuilder(ExpressionBase eprx) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);
		buffer.append(" FROM ");
		buffer.append(expressionBuilder(eprx));

		return buffer.toString();
	}

	/**
	 * Process the GROUP BY clause
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String groupByClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" GROUP BY ", eprx).toString();
	}

	/**
	 * 
	 * @param eprx
	 * @return
	 * @throws AdapterException
	 */
	protected String havingClauseBuilder(List<ExpressionBase> eprx) throws AdapterException {
		return clauseBuilder(" HAVING ", eprx).toString();
	}

	/**
	 * 
	 * @param order
	 * @return
	 * @throws Exception
	 */
	protected String orderClauseBuilder(List<Order> order) throws AdapterException {
		boolean first = true;
		StringBuilder str = new StringBuilder();
		str.append(" ORDER BY ");

		for (Order o : order) {
			if (first) {
				first = false;
			} else {
				str.append(", ");
			}
			str.append(expressionBuilder(o.getExpression()));
			if (o.getOrderType() == Order.Type.ASC) {
				str.append(" ASC");
			} else {
				str.append(" DESC");
			}
		}
		return str.toString();
	}

	/**
	 * Build the where clause
	 * 
	 * @param where
	 * @return
	 * @throws AdapterException
	 */
	protected String whereClauseBuilder(List<ExpressionBase> where) throws AdapterException {
		boolean first = true;
		StringBuilder str = new StringBuilder(" WHERE ");

		for (ExpressionBase exp : where) {
			if (!first) {
				str.append(" AND ");
			}
			str.append("(").append(expressionBuilder(exp)).append(")");
			if (first) {
				first = false;
			}
		}
		return str.toString();
	}

	/**
	 * 
	 * @param setclause
	 * @return
	 * @throws AdapterException
	 */
	protected String setClauseBuilder(List<ExpressionBase> setclause) throws AdapterException {
		boolean first = true;

		StringBuilder str = new StringBuilder();
		for (ExpressionBase exp : setclause) {
			if (!first) {
				str.append(", ");
			}
			str.append(assignClauseBuilder(exp));
			if (first) {
				first = false;
			}
		}
		return str.toString();
	}

	/**
	 * Assign close
	 * 
	 * @param exp
	 * @return
	 * @throws AdapterException
	 */
	protected String assignClauseBuilder(ExpressionBase exp) throws AdapterException {
		StringBuilder str = new StringBuilder();
		ColumnReference col = (ColumnReference) exp;
		str.append(columnNameBuilder(col));
		str.append(" = ");
		str.append(expressionBuilder(col.getColumnValueExp()));
		return str.toString();
	}

	/**
	 * 
	 * @param proj
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionBaseListBuilder(List<ExpressionBase> proj) throws AdapterException {
		boolean first = true;
		StringBuilder str = new StringBuilder();
		for (ExpressionBase exp : proj) {
			if (first) {
				first = false;
			} else {
				str.append(", ");
			}
			str.append(expressionBuilder(exp));
		}
		return str.toString();
	}

	/**
	 * Build BETWEEN statement
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionBETWEENBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		if (Type.BETWEEN.equals(expr.getType())) {
			buffer.append(" BETWEEN ");
		} else if (Type.NOT_BETWEEN.equals(expr.getType())) {
			buffer.append(" NOT BETWEEN ");

		} else {
			throw new AdapterException("Expression type [" + expr.getType().name()
					+ "] not recognize as 'BETWEEN|NOT BETWEEN' statement.");
		}

		boolean first = true;
		for (ExpressionBase base : expr.getOperands()) {
			if (first) {
				first = false;
			} else {
				buffer.append(" AND ");
			}

			buffer.append(expressionBuilder(base));
		}

		return buffer.toString();
	}

	/**
	 * Build LIKE statement
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionLIKEBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.LIKE.equals(expr.getType())) {
			buffer.append(" LIKE ");
		} else if (Type.NOT_LIKE.equals(expr.getType())) {
			buffer.append(" NOT LIKE ");
		} else {
			throw new AdapterException(
					"Expression type [" + expr.getType().name() + "] not recognize as 'LIKE|NOT LIKE' statement.");
		}

		buffer.append(expressionBuilder(expr.getOperands().get(1)));

		return buffer.toString();
	}

	/**
	 * Build CONCAT statement
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionCONCAT(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append("CONCAT (");
		boolean first = true;
		for (ExpressionBase base : expr.getOperands()) {
			if (first) {
				first = false;
			} else {
				buffer.append(',');
			}
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");
		return buffer.toString();
	}

	/**
	 * Build IN clause
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionINBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.IN.equals(expr.getType())) {
			buffer.append(" IN ");
		} else if (Type.NOT_IN.equals(expr.getType())) {
			buffer.append(" NOT IN ");
		} else {
			throw new AdapterException(
					"Expression type [" + expr.getType().name() + "] not recognize as 'IN|NOT IN' statement.");
		}

		buffer.append("(");
		buffer.append(expressionBuilder(expr.getOperands().get(1)));
		buffer.append(")");

		return buffer.toString();
	}

	/**
	 * Create a list of item
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionList(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		boolean first = true;

		if (expr.getOperands() == null || expr.getOperands().isEmpty()) {
			buffer.append(expr.getValue());
		} else {
			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append(",");
				}

				buffer.append(expressionBuilder(b));
			}
		}
		return buffer.toString();
	}
	
	/**
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionAND(Expression expr) throws AdapterException {
		return ORANDExpression(false, expr);
	}

	/**
	 * Create OR expression
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionOR(Expression expr) throws AdapterException {
		return ORANDExpression(true, expr);
	}
	
	protected String ORANDExpression(boolean isOr, Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);
		boolean first = true;

		try {
			for (ExpressionBase b : expr.getOperands()) {
				if (first) {
					first = false;
				} else {
					buffer.append((isOr) ? " OR " : " AND ");
				}

				buffer.append(expressionBuilder(b));

			}
		} catch (Exception e) {
			throw new AdapterException(e);
		}

		return buffer.toString();
	}

	/**
	 * Build DISTINCT syntax
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionDISTINCTBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append("DISTINCT ");
		for (ExpressionBase base : expr.getOperands()) {
			buffer.append(expressionBuilder(base));
		}
		buffer.append(")");

		return buffer.toString();
	}

	/**
	 * Apply subquery
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionSubQuery(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(regenerateSQL(expr.getOperands().get(0)));
		buffer.append(")");

		return buffer.toString();
	}

	/**
	 * Appls subquery
	 * 
	 * @param query
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionSubQuery(Query query) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(regenerateSQL(query));
		buffer.append(")");

		return buffer.toString();
	}

	/**
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionUNIONBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.UNION_ALL.equals(expr.getType())) {
			buffer.append(" UNION ALL ");
		} else if (Type.UNION_DISTINCT.equals(expr.getType())) {
			buffer.append(" UNION ");
		} else if (Type.INTERSECT.equals(expr.getType())) {
			buffer.append(" INTERSECT ");
		} else if (Type.EXCEPT.equals(expr.getType())) {
			buffer.append(" EXCEPT ");
		} else {
			throw new AdapterException(
					"Expression type [" + expr.getType().name() + "] not recognize as 'UNION ALL|UNION' statement.");
		}

		buffer.append(regenerateSQL(expr.getOperands().get(1)));

		return buffer.toString();
	}

	/**
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionNULLOperand(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		int size = expr.getOperands().size();

		buffer.append(expressionBuilder(expr.getOperands().get(0)));
		if (Type.IS_NULL.equals(expr.getType())) {
			buffer.append(" IS NULL ");
		} else if (Type.IS_NOT_NULL.equals(expr.getType())) {
			buffer.append(" IS NOT NULL ");
		} else {
			throw new AdapterException("Expression type [" + expr.getType().name()
					+ "] not recognize as 'IS NULL| IS NOT NULL' statement.");
		}

		if (size > 1) {
			buffer.append(expressionBuilder(expr.getOperands().get(1)));
		}
		return buffer.toString();
	}

	/**
	 * 
	 * @param join
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionJoinBuilder(Join join) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append("(");
		buffer.append(expressionBuilder(join.getLeftNode()));
		if (join.getType() == ExpressionBase.Type.INNER_JOIN) {
			buffer.append(" INNER JOIN ");
		} else if (Type.LEFT_OUTER_JOIN.equals(join.getType())) {
			buffer.append(" LEFT OUTER JOIN ");
		} else {
			throw new AdapterException("Expression type [" + join.getType().name()
					+ "] not recognize as 'INNER JOIN|LEFT OUTER JOIN' statement.");
		}
		buffer.append(expressionBuilder(join.getRightNode()));
		buffer.append(" ON (");
		buffer.append(expressionBuilder(join.getJoinCondition()));
		buffer.append("))");

		return buffer.toString();
	}

	/**
	 * Build statement containing two expression separate by a sign or something
	 * else
	 * 
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String twoMembersBuilder(Expression expr) throws AdapterException {
		StringBuilder str = new StringBuilder();
		try {
			str.append(expressionBuilder((ExpressionBase) expr.getOperands().get(0)));
			str.append(" " + expr.getValue() + " ");
			str.append(expressionBuilder((ExpressionBase) expr.getOperands().get(1)));
		} catch (Exception e) {
			throw new AdapterException(e);
		}
		return str.toString();
	}

	protected String regenerateSQL(ExpressionBase query) throws AdapterException {
		if (query.getType() == ExpressionBase.Type.SELECT) {
			return regenerateSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.INSERT) {
			return regenerateInsertSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.DELETE) {
			return regenerateDeleteSQL((Query) query);
		}
		if (query.getType() == ExpressionBase.Type.UPDATE) {
			return regenerateUpdateSQL((Query) query);
		}
		StringBuilder str = new StringBuilder();
		Expression exp = (Expression) query;
		str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(0)));
		str.append(" ");
		str.append(printSetOperation(query.getType()));
		str.append(" ");
		str.append(expressionBuilder((ExpressionBase) exp.getOperands().get(1)));
		return str.toString();
	}

	protected String regenerateInsertSQL(Query query) throws AdapterException {
		StringBuilder sql = new StringBuilder();

		sql.append("INSERT INTO ");
		sql.append(expressionBuilder(query.getFromClause()));
		if (query.getProjections() != null) {
			sql.append(" (");
			sql.append(expressionBaseListBuilder(query.getProjections()));
			sql.append(")");
		}
		if (query.getValueClause() != null) {
			sql.append(" VALUES ");
			sql.append("(");
			sql.append(expressionBaseListBuilder(query.getValueClause()));
			sql.append(")");
		}
		if (query.getSubquery() != null) {
			sql.append(expressionBuilder(query.getSubquery()));
		}
		return sql.toString();
	}

	protected String regenerateDeleteSQL(Query query) throws AdapterException {
		StringBuilder sql = new StringBuilder();
		sql.append("DELETE FROM ");
		sql.append(expressionBuilder(query.getFromClause()));
		if (query.getWhereClause() != null) {
			sql.append(" WHERE ");
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		return sql.toString();
	}

	protected String regenerateUpdateSQL(Query query) throws AdapterException {
		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ");
		sql.append(expressionBuilder(query.getFromClause()));
		sql.append(" SET ");
		sql.append(setClauseBuilder(query.getProjections()));
		if (query.getWhereClause() != null) {
			sql.append(" WHERE ");
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		return sql.toString();
	}

	protected String regenerateSQL(Query query) throws AdapterException {
		StringBuilder sql = new StringBuilder();

		sql.append("SELECT ");

		if (!this.limitAtEnd) {
			if (query.getLimit() != null) {
				sql.append("TOP ");
				sql.append(query.getLimit());
				sql.append(" ");
			}
		}
		if (query.getDistinct()) {
			sql.append("DISTINCT ");
		}
		sql.append(expressionBaseListBuilder(query.getProjections()));

		sql.append(fromClauseBuilder(query.getFromClause()));

		if (query.getWhereClause() != null) {
			sql.append(whereClauseBuilder(query.getWhereClause()));
		}
		if (query.getGroupBy() != null) {
			sql.append(groupByClauseBuilder(query.getGroupBy()));
		}
		if (query.getHavingClause() != null) {
			sql.append(havingClauseBuilder(query.getHavingClause()));
		}
		if (query.getOrderBy() != null) {
			sql.append(orderClauseBuilder(query.getOrderBy()));
		}

		if (this.limitAtEnd) {
			try {
				Method method = query.getClass().getMethod("getLimit", null);

				Object value = method.invoke(query, null);
				if (value != null) {
					// if (query.getLimit() != null) {
					sql.append(" LIMIT ");
					// sql.append(query.getLimit());
					sql.append(value.toString());
					sql.append(" ");
				}
			} catch (Exception e) {
				logger.error("Failed to retrieve Limit", e);
			}
		}
		return sql.toString();
	}

	public String rewriteSQL(String sql) throws AdapterException {
		List<ExpressionParserMessage> messageList = new ArrayList<ExpressionParserMessage>();
		try {
			ExpressionBase query = ExpressionParserUtil.buildQuery(sql, messageList);
			if (query != null) {
				setQueryType(query.getType());
				String sqlRewrite = regenerateSQL(query);

				return StringUtils.hasText(sqlRewrite) ? sqlRewrite.trim() : null;
			}
			for (ExpressionParserMessage e : messageList) {
				this.logger.error(e.getText());
			}
			throw new AdapterException("Parse failed. See earlier logs");
		} catch (Exception e) {
			this.logger.error("SQL Rewrite failed.", e);
			throw new AdapterException(e, "Parser failed. See earlier logs");
		}
	}

	protected String aliasRewriter(String alias) {
		if (alias.length() <= this.getMaxIdentifierLength()) {
			return alias;
		}
		String newAlias = (String) this.aliasMap.get(alias);
		if (newAlias == null) {
			newAlias = String.valueOf(this.aliasSeed++);
			this.aliasMap.put(alias, newAlias);
		}
		return newAlias;
	}

	protected static String printSetOperation(ExpressionBase.Type type) throws AdapterException {
		String str = new String();
		switch (type) {
		case OR:
			str = "UNION ALL";
			break;
		case LESS_THAN:
			str = "UNION DISTINCT";
			break;
		case AND:
			str = "INTERSECT";
			break;
		case UNION_ALL:
			str = "EXCEPT";
			break;
		default:
			throw new AdapterException("Operation [" + type.name() + "] is not supported.");
		}
		return str;
	}

	protected String printDT(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();

		buffer.setLength(0);

		String _v = ((Expression) expr.getOperands().get(0)).getValue();

		switch (expr.getType()) {
		case TIMESTAMP_LITERAL:
			buffer.append("TIMESTAMP ");
			buffer.append(_v);
			buffer.append("");
			break;
		case DATE_LITERAL:
			buffer.append("DATE ");
			buffer.append(_v);
			buffer.append("");
			break;
		default:
			throw new AdapterException("Expression type [" + expr.getType().name() + "] is not supported.");
		}

		return buffer.toString();
	}

	/**
	 * 
	 * @param tabRef
	 * @return
	 * @throws AdapterException
	 */
	protected String tableNameBuilder(TableReference tabRef) throws AdapterException {
		return UniqueNameTools.build(tabRef.getName()).getUniqueName();
	}

	protected String columnNameBuilder(ColumnReference colRef) {
		StringBuilder str = new StringBuilder();
		if (colRef.getTableName() != null) {
			str.append(aliasRewriter(colRef.getTableName()) + ".");
		}

		if ("*".equalsIgnoreCase(colRef.getColumnName())) {
			str.append("*");
		} else {
			str.append(colRef.getColumnName().replaceAll("\"", ""));
		}

		return str.toString();
	}

	protected String expressionBuilder(ExpressionBase val) throws AdapterException {
		StringBuilder str = new StringBuilder();

		switch (val.getType()) {
		case ALL:
			str.append(expressionList((Expression) val));
			break;
		case COLUMN:
			str.append(columnNameBuilder((ColumnReference) val));
			break;
		case AND:
			str.append(expressionAND((Expression)val));
			
//			if (val instanceof TableReference) {
//				str.append(tableNameBuilder((TableReference) val));
//			} else {
//				str.append(expressionAND((Expression)val));
//			}
			break;
		case TABLE:
			str.append(tableNameBuilder((TableReference) val));
			break;
		case SUBQUERY:
			str.append(" ( ");
			str.append(regenerateSQL((Query) val));
			str.append(" ) ");
			break;
		case TIMESTAMP_LITERAL:
		case DATE_LITERAL:
		case TIME_LITERAL:
			str.append(printDT((Expression) val));
			break;
		case GREATER_THAN:
		case EQUAL:
		case DIVIDE:
		case GREATER_THAN_EQ:
		case SUBTRACT:
		case MULTIPLY:
		case NOT_EQUAL:
		case LESS_THAN:
		case ADD:
		case LESS_THAN_EQ:
			str.append(twoMembersBuilder((Expression) val));
			break;
		case IN:
		case NOT_IN:
			str.append(expressionINBuilder((Expression) val));
			break;
		case CHARACTER_LITERAL:
		case INT_LITERAL:
		case FLOAT_LITERAL:
			str.append(((Expression) val).getValue());
			break;
		case OR:
			str.append(expressionOR((Expression) val));
			break;
		case FUNCTION:
			str.append(expressionSQLFunctionsBuilder((Expression) val));
			break;
		case LEFT_OUTER_JOIN:
		case INNER_JOIN:
			str.append(expressionJoinBuilder((Join) val));
			break;
		case LIKE:
		case NOT_LIKE:
			str.append(expressionLIKEBuilder((Expression) val));
			break;
		case IS_NULL:
		case IS_NOT_NULL:
			str.append(expressionNULLOperand((Expression) val));
			break;
		case UNION_ALL:
		case UNION_DISTINCT:
		case INTERSECT:
		case EXCEPT:
			str.append(expressionUNIONBuilder((Expression) val));
			break;
		case SELECT:
		case QUERY:
			str.append(expressionSubQuery(((Expression) val)));
			break;
		case DISTINCT:
			expressionDISTINCTBuilder((Expression) val);
			break;
		case BETWEEN:
		case NOT_BETWEEN:
			str.append(expressionBETWEENBuilder((Expression) val));
			break;
		case CONCAT:
			str.append(expressionCONCAT((Expression) val));
			break;
		case CASE:
			str.append(expressionCASE((Expression) val));
			break;
		case NULL:
		case DELETE:
		case ORDER_BY:
		case VARIABLE:
		case ASSIGN:

		case PARAMETER:
		case UNKNOWN:
		case UPDATE:
		case CASE_CLAUSE:
		case CASE_CLAUSES:
		case CASE_ELSE:
			// case ROW_NUMBER:
		case UNARY_POSITIVE:
		case INSERT:
		default:
			throw new AdapterException("Unknown value [" + ((Expression) val).getValue() + "]");
		}
		if (val.getAlias() != null) {
			str.append(" ");
			str.append(aliasRewriter(val.getAlias()));
		}
		return str.toString();
	}

	private String expressionCASE(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		buffer.setLength(0);

		buffer.append(" CASE ");

		for (ExpressionBase base : expr.getOperands()) {
			switch (base.getType()) {
			case CASE_CLAUSES:
				for (ExpressionBase elem : ((Expression) base).getOperands()) {
					buffer.append(" WHEN ");
					buffer.append(expressionBuilder(((Expression) elem).getOperands().get(0)));
					buffer.append(" THEN ");
					buffer.append(expressionBuilder(((Expression) elem).getOperands().get(1)));
				}
				break;
			case CASE_ELSE:
				buffer.append(" ELSE ");
				buffer.append(expressionBuilder(((Expression) base).getOperands().get(0)));
				break;
			default:
				throw new AdapterException("Unsupported type in case expression. [" + base.getType() + "]");
			}
		}

		buffer.append(" END ");
		return buffer.toString();

	}

	/**
	 * Build function expression
	 * 
	 * @param expr
	 * @return
	 * @throws AdapterException
	 */
	protected String expressionSQLFunctionsBuilder(Expression expr) throws AdapterException {
		StringBuilder buffer = new StringBuilder();
		// buffer.setLength(0);
		FUNCTIONS fx = FUNCTIONS.valueOf(expr.getValue());
		try {
			switch (fx) {
			case TO_DOUBLE:
				buffer.append("CAST(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(" AS DOUBLE PRECISION");
				buffer.append(")");
				break;
			case TO_DECIMAL:
				List<ExpressionBase> params = expr.getOperands();
				if (params.size() < 3) {
					buffer.append("CAST(");
					buffer.append(expressionBuilder(expr.getOperands().get(0)));
					buffer.append(" AS DECIMAL");
					buffer.append(")");
				} else {
					buffer.append("CAST(");
					buffer.append(expressionBuilder(expr.getOperands().get(0)));
					buffer.append(" AS DECIMAL(");
					buffer.append(expressionBuilder(expr.getOperands().get(1)));
					buffer.append(",");
					buffer.append(expressionBuilder(expr.getOperands().get(2)));
					buffer.append(")");
					buffer.append(")");
				}
				break;
			case TO_REAL:
			case TO_INT:
			case TO_INTEGER:
			case TO_SMALLINT:
			case TO_TINYINT:
			case TO_BIGINT:
			case TO_TIMESTAMP:
			case TO_DATE:
				buffer.append("CAST(");
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				buffer.append(" AS ");
				buffer.append(fx.suffix());
				buffer.append(")");
				break;
			case MOD:
				buffer.append("(").append(expressionBuilder(expr.getOperands().get(0))).append(" % ")
						.append(expressionBuilder(expr.getOperands().get(1))).append(")");
				break;
			case ATAN2:
			case STDDEV:
			case AVG:
				if (expr.getValue().equals("ATAN2")) {
					buffer.append("ATN2 (");
				} else if (expr.getValue().equals("STDDEV")) {
					buffer.append("STDEV (");
				} else if (expr.getValue().equals("AVG")) {
					buffer.append("AVG (");
				} else {
					buffer.append(expr.getValue() + "(");
				}
				boolean first = true;
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
				/*
				 * if (expr.getValue().equals("AVG")) { buffer.append(" * 1.0 )"); } else {
				 * buffer.append(")"); }
				 */
				buffer.append(")");
				break;
			case TO_VARCHAR:
			case TO_NVARCHAR:
				buffer.append(expressionBuilder(expr.getOperands().get(0)));
				/*
				 * if (columnHelper != null) { buffer.append("CAST (");
				 * buffer.append(printExpression(expr.getOperands().get(0)));
				 * buffer.append(" AS VARCHAR)"); } else { buffer.append("-- " +
				 * expr.getValue()); }
				 */
				break;
			case CONCAT:
				buffer.append(expressionCONCAT(expr));
				break;
			case SUM:
			case COUNT:
			case MIN:
			case MAX:
			case LN:
			case LOG:
			case CEIL:
			case TRIM:
			case LTRIM:
			case RTRIM:
			case UPPER:
			case LOWER:
				buffer.append(printFxColumns(expr));
				break;
			default:
				throw new AdapterException("Function [" + expr.getValue() + "] is not supported.");
			}
		} catch (Exception e) {
			throw new AdapterException(e, "Error while building function [" + expr.getValue() + "].");
		}

		return buffer.toString();
	}

}
