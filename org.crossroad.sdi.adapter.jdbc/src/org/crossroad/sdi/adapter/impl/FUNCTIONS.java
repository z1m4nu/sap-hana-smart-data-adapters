package org.crossroad.sdi.adapter.impl;

public enum FUNCTIONS {
	// Aggregate
	MAX, 
	MIN, 
	SUM, 
	AVG, 
	COUNT,
	// Numeric
	ADD,  
	ROUND, 
	MOD, 
	CEIL, 
	LN, 
	LOG, 
	ATAN2, 
	STDDEV, 
	// String
	CONCAT,
	TRIM,
	LTRIM,
	RTRIM,
	UPPER,
	LOWER,
	// Conversion
	TO_DECIMAL, 
	TO_VARCHAR, 
	TO_NVARCHAR,
	TO_INT, 
	TO_TINYINT, 
	TO_INTEGER, 
	TO_SMALLINT, 
	TO_BIGINT, 
	TO_REAL, 
	TO_DOUBLE, 
	TO_TIMESTAMP,
	TO_DATE;
	static FUNCTIONS fromString(String x) throws Exception {

		for (FUNCTIONS currentType : FUNCTIONS.values()) {
			if (x.equalsIgnoreCase(currentType.toString())) {
				return currentType;
			}
		}
		throw new Exception("Unmatched Type");
	}

	public String suffix() {
		if (name().startsWith("TO_")) {
			int index = name().lastIndexOf('_');
			if (index > -1) {
				return name().substring(++index);
			} else {
				return name();
			}
		} else {
			return name();
		}
	}
}
