package org.crossroad.sdi.adapter.impl;

import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public final class ClassUtil {

	private ClassUtil() {
	}

	/**
	 * 
	 * @param context
	 * @return
	 */
	public static List<String> getDriverClass() {
		List<String> driverClass = new ArrayList<String>();
		ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class,
				ClassUtil.class.getClassLoader());

		for (Driver c : loader) {
			driverClass.add(c.getClass().getName());
		}

		return driverClass;
	}


	public static void main(String[] args) throws Exception {
		ClassUtil.getDriverClass();
	}
}
