/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.jdbc;

import java.util.HashMap;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapter;
import org.crossroad.sdi.adapter.impl.AdapterConstants;
import org.crossroad.sdi.adapter.impl.ClassUtil;
import org.crossroad.sdi.adapter.impl.RemoteSourceDescriptionFactory;

import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

/**
 * JDBCAdapter Adapter.
 */
public class JDBCAdapter extends AbstractJDBCAdapter {

	public JDBCAdapter() {
		super();
	}
	@Override
	protected void populateCFGDriverList(PropertyEntry drvList) throws AdapterException {

		for (String s : ClassUtil.getDriverClass()) {
			logger.info("Adding driver class [" + s + "]");
			drvList.addChoice(s, s);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#doCloseResultSet()
	 */
	public void doCloseResultSet() throws AdapterException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#onClose()
	 */
	public void onClose() {
		logger.info("Closing local data");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.crossroad.sdi.adapter.impl.IJDBCAdapter#getJdbcUrl(com.sap.hana.dp.
	 * adapter.sdk.PropertyGroup)
	 */
	public String getJdbcUrl(PropertyGroup main) throws AdapterException {
		return main.getPropertyEntry(AdapterConstants.KEY_JDBC_URL).getValue();
	}

	@Override
	public Class getLoggerName() {
		return JDBCAdapter.class;
	}

	public RemoteSourceDescription getRemoteSourceDescription() throws AdapterException {
		RemoteSourceDescription rs = new RemoteSourceDescription();
		PropertyGroup mainGroup = RemoteSourceDescriptionFactory.getJDBCConnectionGroup();
		mainGroup.setDisplayName("JDBC Server connection definition");

/*
		PropertyEntry entry = new PropertyEntry(AdapterConstants.KEY_JDBC_TYPE, "Database behavior",
				"Select the database behavior");
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("org.crossroad.sdi.adapter.db.jdbc.DBInfo", "Generic JDBC");
		map.put("org.crossroad.sdi.adapter.db.mssql.DBInfo", "Microsoft SQL Server");
		map.put("org.crossroad.sdi.adapter.db.mysql.DBInfo", "MySQL Server");

		entry.setChoices(map);
		entry.setDefaultValue(AdapterConstants.KEY_JDBC_TYPE_DEFAULT);

		mainGroup.addProperty(entry);
		*/

		mainGroup.addProperty(new PropertyEntry(AdapterConstants.KEY_DATAMAPPING_FILE, "Custom mapping file", "Mapping file", false));
		
		mainGroup.addProperty(RemoteSourceDescriptionFactory.getOptionDisplaySystemTables());
		mainGroup.addProperty(RemoteSourceDescriptionFactory.getOptionNullableAsEmpty());
		
		rs.setCredentialProperties(RemoteSourceDescriptionFactory.getCredentialProperties());
		rs.setConnectionProperties(mainGroup);
		return rs;
	}

}
