/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.jdbc;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class JDBCFactory implements AdapterFactory{

	private static final String NAME = "DEBUGJDBC";//"Generic JDBC";
	@Override
	public Adapter createAdapterInstance() {
		return new JDBCAdapter();
	}

	@Override
	public String getAdapterType() {
		return NAME;
	}

	@Override
	public String getAdapterDisplayName() {
		return NAME;
	}


	@Override
	public String getAdapterDescription() {
		return "Basic JDBC connection no optimisation";
	}

	@Override
	public RemoteSourceDescription getAdapterConfig() {
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		return true;
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription propertyGroup)
			throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}



}
