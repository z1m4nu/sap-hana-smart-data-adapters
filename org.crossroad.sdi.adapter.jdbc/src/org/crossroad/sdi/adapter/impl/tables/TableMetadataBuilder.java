/**
 * 
 */
package org.crossroad.sdi.adapter.impl.tables;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.crossroad.sdi.adapter.impl.ColumnBuilder;
import org.crossroad.sdi.adapter.impl.ColumnHelper;
import org.crossroad.sdi.adapter.impl.UniqueNameTools;
import org.crossroad.sdi.adapter.utils.StringUtils;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Capabilities;
import com.sap.hana.dp.adapter.sdk.Column;
import com.sap.hana.dp.adapter.sdk.ForeignKey;
import com.sap.hana.dp.adapter.sdk.Index;
import com.sap.hana.dp.adapter.sdk.TableMetadata;
import com.sap.hana.dp.adapter.sdk.UniqueKey;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.ColumnCapability;
import com.sap.hana.dp.adapter.sdk.AdapterConstant.TableCapability;

/**
 * @author e.soden
 *
 */
public class TableMetadataBuilder {
	private ColumnBuilder columnBuilder = new ColumnBuilder();
	private ColumnHelper columnHelper = new ColumnHelper();

	/**
	 * 
	 * @param mapperDefault
	 * @param mapperCustom
	 * @throws AdapterException
	 */
	public void init(String mapperDefault, String mapperCustom) throws AdapterException {
		columnBuilder.loadMapping(mapperDefault, mapperCustom);
	}

	public TableMetadata createMetaData(DatabaseMetaData meta, String tableuniquename) throws AdapterException {
		try {
			/*
			 * nodeId does match the format: catalog.schema.tablename
			 */
			UniqueNameTools tools = UniqueNameTools.build(tableuniquename);

			if ((tools.getCatalog() == null && tools.getSchema() == null)) {
				throw new AdapterException(
						"Unique Name of the table does not match the format catalog.schema.tablename: "
								+ tableuniquename);
			}

			if (tools.getTable() == null) {
				throw new AdapterException("Table Name portion cannot be empty: " + tableuniquename);
			}
			
			

			TableMetadata metas = new TableMetadata();
			metas.setName(tools.getUniqueName());
			metas.setPhysicalName(tools.getTable());
			metas.setColumns(buildColumns(meta, tools));
			metas.setUniqueKeys(buildUniqueKeys(meta, tools));
			metas.setIndices(buildIndices(meta, tools));
			metas.setForeignKeys(buildForeignKeys(meta, tools));
			
			
			/*
			 * Set primary columns
			 */
			List<Column> columns = metas.getColumns();
			List<UniqueKey> keys = metas.getUniqueKeys();
			for (UniqueKey key : keys) {
				List<String> columnNames = key.getColumnNames();
				for (Column column : columns)
					if (columnNames.contains(column.getName()))
						column.setPrimaryKey(true);
			}
			
			
			
			Capabilities<TableCapability> caps = new Capabilities<TableCapability>();
			caps.setCapability(TableCapability.CAP_TABLE_SELECT);
			caps.setCapability(TableCapability.CAP_TABLE_COLUMN_CAP);
			caps.setCapability(TableCapability.CAP_TABLE_AND);
			caps.setCapability(TableCapability.CAP_TABLE_AND_DIFFERENT_COLUMNS);
			caps.setCapability(TableCapability.CAP_TABLE_OR);
			caps.setCapability(TableCapability.CAP_TABLE_OR_DIFFERENT_COLUMNS);

			caps.setCapability(TableCapability.CAP_TABLE_LIMIT);

			metas.setCapabilities(caps);

			return metas;
		} catch (AdapterException e) {
			throw e;
		} catch (Exception e) {
			throw new AdapterException(e, String.format("Error while processing (%s)", tableuniquename));
		}
	}

	/**
	 * 
	 * @param meta
	 * @param tableuniquename
	 * @return
	 * @throws AdapterException
	 */
	public static TableMetadata factory(DatabaseMetaData meta, String tableuniquename) throws AdapterException {
		TableMetadataBuilder builder = new TableMetadataBuilder();
		return builder.createMetaData(meta, tableuniquename);
	}

	private List<ForeignKey> buildForeignKeys(DatabaseMetaData meta, UniqueNameTools tools) throws AdapterException {
		ResultSet rs = null;
		try {
			List<ForeignKey> data = new LinkedList<>();
			
			rs = meta.getExportedKeys(tools.getCatalog(), tools.getSchema(), tools.getTable());
			while(rs.next())
			{
				data.add(new ForeignKey(rs.getString("FK_NAME"), rs.getString("PK_NAME")));
			}
			return data;
		} catch (SQLException e) {
			throw new AdapterException(e, String.format("Error while retrieving foreign keys for '%s'", tools.toString()));
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
			}

			rs = null;
		}
	}
	/**
	 * 
	 * @param meta
	 * @param tools
	 * @return
	 * @throws AdapterException
	 */
	private List<UniqueKey> buildUniqueKeys(DatabaseMetaData meta, UniqueNameTools tools) throws AdapterException {

		ResultSet rs = null;

		try {
			HashMap<String, List<String>> map = new HashMap<String, List<String>>();
			List<UniqueKey> uniqueKeys = new LinkedList<>();

			rs = meta.getPrimaryKeys(tools.getCatalog(), tools.getSchema(), tools.getTable());

			while (rs.next()) {
				String indexName = rs.getString("PK_NAME");
				if (indexName == null)
					continue;
				String fieldName = rs.getString("COLUMN_NAME");
				if (!map.containsKey(indexName))
					map.put(indexName, new LinkedList<>());
				map.get(indexName).add(fieldName);
			}

			for (String key : map.keySet()) {
				UniqueKey uniqueKey = new UniqueKey(key, map.get(key));
				uniqueKey.setPrimary(true);
				uniqueKeys.add(uniqueKey);
			}

			return uniqueKeys;
		} catch (SQLException e) {
			throw new AdapterException(e, String.format("Error while retrieving unique keys for '%s'", tools.toString()));
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
			}

			rs = null;
		}

	}

	/**
	 * 
	 * @param meta
	 * @param tools
	 * @return
	 * @throws AdapterException
	 */
	private List<Column> buildColumns(DatabaseMetaData meta, UniqueNameTools tools) throws AdapterException {
		ResultSet rs = null;

		List<Column> cols = new ArrayList<Column>();
		try {

			rs = meta.getColumns(tools.getCatalog(), tools.getSchema(), tools.getTable(), null);

			while (rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				int columnType = rs.getInt("DATA_TYPE");
				String typeName = rs.getString("TYPE_NAME");
				int size = rs.getInt("COLUMN_SIZE");
				int nullable = rs.getInt("NULLABLE");
				int scale = rs.getInt("DECIMAL_DIGITS");
				String remarks = rs.getString("REMARKS");
				String autoIncrement = rs.getString("IS_AUTOINCREMENT");

				Column column = columnBuilder.createColumn(columnName, columnType, typeName, size, size, scale);

				column.setNullable(nullable == 1);
				column.setDescription(remarks);
				if(StringUtils.hasText(autoIncrement))
				{
					column.setAutoIncrement("YES".equalsIgnoreCase(autoIncrement));
				}

				columnHelper.addColumn(column, columnType);
				Capabilities<ColumnCapability> columnCaps = new Capabilities<>();
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_SELECT);

				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_OUTER_JOIN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_INNER_JOIN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_SORT);

				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_BETWEEN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_FILTER);

				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_IN);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_LIKE);
				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_NONEQUAL_COMPARISON);

				columnCaps.setCapability(ColumnCapability.CAP_COLUMN_GROUP);

				column.setCapabilities(columnCaps);

				cols.add(column);

			}

		} catch (SQLException e) {
			throw new AdapterException(e, String.format("Error while retrieving column information for '%s'", tools.toString()));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
				rs = null;
			}
		}

		return cols;
	}

	/**
	 * 
	 * @param meta
	 * @param tools
	 * @return
	 * @throws AdapterException
	 */
	private List<Index> buildIndices(DatabaseMetaData meta, UniqueNameTools tools) throws AdapterException {
		List<Index> indices = new ArrayList<Index>();
		ResultSet rs = null;

		try {
			rs = meta.getIndexInfo(tools.getCatalog(), tools.getSchema(), tools.getTable(), false, true);

			HashMap<String, Index> map = new HashMap<String, Index>();
			while (rs.next()) {
				String indexname = rs.getString("INDEX_NAME");

				if (!map.containsKey(indexname)) {
					Index index = new Index(indexname);
					List<String> columns = new ArrayList<String>();
					columns.add(rs.getString("COLUMN_NAME"));
					index.setColumnNames(columns);
					map.put(indexname, index);
				} else {
					Index index = map.get(indexname);
					index.getColumnNames().add(rs.getString("COLUMN_NAME"));
				}
			}

			for (Index index : map.values()) {
				indices.add(index);
			}

		} catch (SQLException e) {
			throw new AdapterException(e, String.format("Error while retrieving indices for '%s'", tools.toString()));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
				rs = null;
			}
		}

		return indices;
	}
}
