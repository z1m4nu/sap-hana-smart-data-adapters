package org.crossroad.sdi.adapter.impl;

public class TableName {
	private String owner;
	private String tableName;

	public TableName(String qualifiedTableName) {
		if (qualifiedTableName.contains("\"")) {
			int loc = 0;
			if (qualifiedTableName.contains("\".\"")) {
				loc = qualifiedTableName.indexOf("\".\"");
				this.owner = qualifiedTableName.substring(0, loc + 1);
				this.tableName = qualifiedTableName.substring(loc + 2);
			} else if ((qualifiedTableName.startsWith("\"")) && (!qualifiedTableName.endsWith("\""))) {
				loc = qualifiedTableName.lastIndexOf("\".");
				this.owner = qualifiedTableName.substring(0, loc + 1);
				this.tableName = qualifiedTableName.substring(loc + 2).replaceAll("\"", "");
			} else if ((qualifiedTableName.startsWith("\"\"")) && (!qualifiedTableName.endsWith("\"\""))) {
				loc = qualifiedTableName.lastIndexOf("\".");
				this.owner = qualifiedTableName.substring(0, loc);
				this.tableName = qualifiedTableName.substring(loc + 2).replaceAll("\"", "");
			} else if ((!qualifiedTableName.startsWith("\"")) && (qualifiedTableName.endsWith("\""))) {
				loc = qualifiedTableName.indexOf(".");
				this.owner = qualifiedTableName.substring(0, loc).replaceAll("\"", "");
				this.tableName = qualifiedTableName.substring(loc + 1);
			} else if ((!qualifiedTableName.startsWith("\"\"")) && (qualifiedTableName.endsWith("\"\""))) {
				loc = qualifiedTableName.indexOf(".");
				this.owner = qualifiedTableName.substring(0, loc).replaceAll("\"", "");
				this.tableName = qualifiedTableName.substring(loc + 1);
			} else if (!qualifiedTableName.contains(".")) {
				this.owner = null;
				this.tableName = qualifiedTableName.replaceAll("\"", "");
			} else {
				loc = qualifiedTableName.indexOf('.');
				this.owner = qualifiedTableName.substring(0, loc).replaceAll("\"", "");
				this.tableName = qualifiedTableName.substring(loc + 1).replaceAll("\"", "");
			}
			if ((this.owner != null) && ((this.owner.startsWith("\"")) || (this.owner.endsWith("\"")))) {
				this.owner = this.owner.substring(1, this.owner.length() - 1);
			}
			if ((this.tableName.startsWith("\"")) || (this.tableName.endsWith("\""))) {
				this.tableName = this.tableName.substring(1, this.tableName.length() - 1);
			}
		} else if (qualifiedTableName.contains(".")) {
			int loc = qualifiedTableName.indexOf('.');
			this.owner = qualifiedTableName.substring(0, loc);
			this.tableName = qualifiedTableName.substring(loc + 1);
		} else {
			this.owner = null;
			this.tableName = qualifiedTableName;
		}
	}

	public TableName(String owner, String tableName) {
		this.owner = owner;
		this.tableName = tableName;
	}

	public String getOwner() {
		return this.owner;
	}

	public String getTableName() {
		return this.tableName;
	}

	String getFullName() {
		return this.owner + "." + this.tableName;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
