/**
 * (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.crossroad.sdi.adapter.jdbc;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import com.sap.hana.dp.adapter.sdk.AdapterFactory;

public class Activator  implements BundleActivator {

	private static BundleContext context;

	static BundleContext getContext() {
		return context;
	}

	ServiceRegistration<?> adapterRegistration;
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	public void start(BundleContext bundleContext) throws Exception {
		Activator.context = bundleContext;
		JDBCFactory srv = new JDBCFactory();
		adapterRegistration = context.registerService(AdapterFactory.class.getName(),srv ,null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext bundleContext) throws Exception {
		Activator.context = null;
		adapterRegistration.unregister();
	}

}