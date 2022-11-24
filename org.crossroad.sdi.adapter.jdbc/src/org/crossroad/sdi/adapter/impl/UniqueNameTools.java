/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import org.crossroad.sdi.adapter.utils.StringUtils;

/**
 * @author e.soden
 *
 */
public class UniqueNameTools {

	private String catalog = null;
	private String schema = null;
	private String table = null;

	protected UniqueNameTools() {
	}

	/**
	 * @return the catalog
	 */
	public String getCatalog() {
		return (this.catalog == AdapterConstants.NULL_AS_STRING) ? null : catalog;
	}

	/**
	 * @return the schema
	 */
	public String getSchema() {
		return (this.schema == AdapterConstants.NULL_AS_STRING) ? null : schema;
	}

	/**
	 * @return the table
	 */
	public String getTable() {
		return (this.table == AdapterConstants.NULL_AS_STRING) ? null : table;
	}

	public static UniqueNameTools build(String uniqueName) {
		UniqueNameTools cls = new UniqueNameTools();
		if (uniqueName != null) {
			uniqueName = uniqueName.replace("\"", "").replace("<none>.", "").replace(".<none>", "");

			String split[] = uniqueName.split("\\.");

			switch (split.length) {
			case 1:
				cls.table = split[0];
				break;
			case 2:
				cls.schema = split[0];
				cls.table = split[1];
				break;
			case 3:
				cls.catalog = split[0];
				cls.schema = split[1];
				cls.table = split[2];
				break;
			default:
				break;
			}
		}

		return cls;
	}

	public String getUniqueName() {
		StringBuilder buffer = new StringBuilder();

		if (StringUtils.hasText(catalog)) {
			buffer.append(catalog);
		}

		if (StringUtils.hasText(schema)) {
			if (buffer.length() > 0) {
				buffer.append(".");
			}
			buffer.append(this.schema);
		}

		if (StringUtils.hasText(table)) {
			if (buffer.length() > 0) {
				buffer.append(".");
			}
			buffer.append(this.table);
		}

		return buffer.toString();
	}
}
