/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.crossroad.sdi.adapter.data.mappings.IDataMapping;

import com.sap.hana.dp.adapter.sdk.AdapterConstant.DataType;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.Column;

/**
 * @author e.soden
 *
 */
public class ColumnBuilder {
	static final Logger logger = LogManager.getLogger("ColumnBuilder");

	private Properties mappingTable = new Properties();

	/**
	 * 
	 */
	public ColumnBuilder() {

	}

	/**
	 * Load the different data mapping properties
	 * 1 - DB Default data mapping
	 * 2 - Custom data mapping
	 * @param dbDefaultMapping
	 * @param customMapping
	 * @throws AdapterException
	 */
	public void loadMapping(String dbDefaultMapping, String customMapping) throws AdapterException {
		try {
			logger.info("DB Default Mapping ["+dbDefaultMapping+"]");
			logger.info("Custom Mapping ["+customMapping+"]");


			if (dbDefaultMapping != null) {
				mappingTable.load(IDataMapping.class.getResourceAsStream(dbDefaultMapping));
				
				if (customMapping != null && customMapping.length() >0)
				{
					mappingTable.load(new FileInputStream(customMapping));
				}
			}

			if (mappingTable.isEmpty()) {
				throw new AdapterException("Data mapping not loaded or empty");
			}

		} catch (Exception e) {
			throw new AdapterException(e);
		}

	}

	public Column createColumn(String name, int jdbcType, String jdbcTypeName, int length, int precision, int scale) {

		Column column = new Column();

		column.setName(name);

		if (precision > 0) {
			column.setPrecision(precision);
		}

		if (scale > 0) {
			column.setScale(scale);
		}

		column.setLength(length);
		column.setNativeLength(length);

		jdbcTypeName = jdbcTypeName.toUpperCase().replace(" ", "");
		
		
		
		column.setDataType(DataType.valueOf(mappingTable.getProperty(jdbcTypeName, DataType.INVALID.name())));

		logger.debug("Name [" + name + "] JDBC Type [" + jdbcType + "] JDBC Name [" + jdbcTypeName + "] HANA Type ["
				+ column.getDataType().name() + "]");
		return column;
	}
}
