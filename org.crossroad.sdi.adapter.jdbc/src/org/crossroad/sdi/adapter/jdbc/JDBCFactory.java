/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.jdbc;

import org.crossroad.sdi.adapter.impl.AbstractJDBCAdapterFactory;
import org.crossroad.sdi.adapter.impl.RequiredComponents;
import org.osgi.framework.BundleContext;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterException;
import com.sap.hana.dp.adapter.sdk.RemoteSourceDescription;

public class JDBCFactory extends AbstractJDBCAdapterFactory {

	private static final String NAME = "JDBC Generic Adapter";// "Generic JDBC";

	public JDBCFactory(BundleContext context) {
		super(context);
	}

	@Override
	public RemoteSourceDescription getAdapterConfig() throws AdapterException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAdapterDescription() {
		return "DP JDBC Adapter";
	}

	@Override
	public String getAdapterDisplayName() {
		return NAME;
	}

	@Override
	public String getAdapterType() {
		return "JDBC Generic adapter";
	}

	@Override
	public RemoteSourceDescription upgrade(RemoteSourceDescription arg0) throws AdapterException {
		return null;
	}

	@Override
	public boolean validateAdapterConfig(RemoteSourceDescription arg0) throws AdapterException {
		return false;
	}

	@Override
	public RequiredComponents getRequiredComponents() {
		 return null;
	}

	@Override
	protected Adapter doCreateAdapterInstance() {
		return (Adapter) new JDBCAdapter();
	}

}
