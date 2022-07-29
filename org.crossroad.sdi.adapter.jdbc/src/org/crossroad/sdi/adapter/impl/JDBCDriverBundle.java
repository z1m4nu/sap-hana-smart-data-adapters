/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.io.File;
import java.io.FileFilter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

/**
 * @author e.soden
 *
 */
public class JDBCDriverBundle {
	static Logger logger = LogManager.getLogger(JDBCDriverBundle.class);
	
	private final BundleContext context;
	
	/**
	 * 
	 */
	public JDBCDriverBundle(BundleContext context) {
		this.context = context;

	}

	public void LoadExternalLibs() throws BundleException {

		File file = new File("lib");

		logger.info("Check jar files in '" + file.getAbsolutePath() + "'");

		if ((!file.exists()) || (!file.isDirectory())) {
			throw new BundleException("Additional jar files '" + file.getAbsolutePath()
					+ "' does not exist. Switch to embedded jar file.");
		} else {

	
			File[] jars = file.listFiles(new FileFilter() {

				@Override
				public boolean accept(File pathname) {
					boolean bReturn = false;
					if (!pathname.isDirectory()) {
						return pathname.getName().endsWith(".jar");
					} else {
						bReturn = false;
					}
					return bReturn;
				}
			});

			for (File jarFile : jars) {
				logger.info("Installing and starting bundle from ["
						+ jarFile.getName() + "].");
				createBundleFromJarFile(jarFile.getAbsolutePath());
			}
		}

	}

	private void createBundleFromJarFile(String jarLocation) throws BundleException {

		try {

			File fUrl = new File(jarLocation);
			String url = fUrl.toURI().toURL().toExternalForm();
			logger.info("Load jar [" + url + "] as bundle.");
			// String url = "wrap:file:lib/" + fUrl.getName();
			Bundle jdbcBundle = this.context.installBundle(url);

			jdbcBundle.start();

		} catch (Exception e) {
			throw new BundleException("Unable to load bundle", e);
		}
	}

}
