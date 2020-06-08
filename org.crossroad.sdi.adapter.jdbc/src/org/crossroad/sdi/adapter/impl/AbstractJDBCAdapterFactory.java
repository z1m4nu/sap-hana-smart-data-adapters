package org.crossroad.sdi.adapter.impl;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import com.sap.hana.dp.adapter.sdk.Adapter;
import com.sap.hana.dp.adapter.sdk.AdapterFactory;

public abstract class AbstractJDBCAdapterFactory implements AdapterFactory {
	private static volatile boolean bundleLoadedInstalled;
	static Logger logger = LogManager.getLogger(AbstractJDBCAdapterFactory.class);
	
	private final BundleContext context;

	private RequiredComponents components = null;

	public AbstractJDBCAdapterFactory(BundleContext context) {
		this.context = context;
		
	}
	
	
	protected void loadExternalLibraries() throws JDBCDriverException {
		if (!bundleLoadedInstalled) {
			this.components = getRequiredComponents();
			try {
				LoadExternalLibs();
			} catch (BundleException e1) {
				logger.error("Failed to install driver", e1);
			}

			try {
				checkDriver();
			} catch (CheckClassException e1) {
				throw new JDBCDriverException(e1);
			}
			bundleLoadedInstalled = true;
		} else {
			try {
				checkDriver();
			} catch (CheckClassException e1) {
				throw new JDBCDriverException(e1);
			}
		}
	}

	private void checkDriver() throws CheckClassException {

		for (Entry<String[], Boolean> raw : this.components.getRequiredClasses()) {
			int nbrSuccess = 0;

			for (String cl : raw.getKey()) {
				logger.debug("Checking class [" + cl + "].");
				try {
					Class.forName(cl);
					nbrSuccess++;
				} catch (Exception e) {
					logger.error("Unable to load required class '" + cl + "'.");
					
					if (raw.getValue().booleanValue()) {
						throw new CheckClassException("Unable to load required class '" + cl + "'.", e);
					}
					
				}

			}

			if (!raw.getValue().booleanValue() && nbrSuccess == 0) {
				throw new CheckClassException("Required classes are not loaded.");
			}
		}

	}

	private void LoadExternalLibs() throws BundleException {

		File file = new File("lib");

		logger.info("Check jar files in '" + file.getAbsolutePath() + "'");

		if ((!file.exists()) || (!file.isDirectory())) {
			throw new BundleException("Additional jar files '" + file.getAbsolutePath()
					+ "' does not exist. Switch to embedded jar file.");
		} else {

			final List<Pattern> patterns = new ArrayList<Pattern>();
			for (String sPattern : this.components.getLibPattern()) {
				patterns.add(Pattern.compile(sPattern));
			}

			File[] jars = file.listFiles(new FileFilter() {

				@Override
				public boolean accept(File pathname) {
					boolean bReturn = false;
					if (!pathname.isDirectory()) {
						for (Pattern pattern : patterns) {
							if (pattern.matcher(pathname.getName()).matches()) {
								bReturn = true;
							}
						}
					} else {
						bReturn = false;
					}
					return bReturn;
				}
			});

			for (File jarFile : jars) {
				logger.info("["+getAdapterDisplayName()+"] - Installing and starting bundle from [" + jarFile.getName() + "].");
				createBundleFromJarFile(jarFile.getAbsolutePath());
			}
		}

	}

	private void createBundleFromJarFile(String jarLocation) throws BundleException {

		try {
			
			File fUrl = new File(jarLocation);
			String url = fUrl.toURI().toURL().toExternalForm();
			logger.info("Load jar ["+url+"] as bundle.");
			//String url = "wrap:file:lib/" + fUrl.getName();
			Bundle jdbcBundle = this.context.installBundle(url);
			
			jdbcBundle.start();
			
			
			
		} catch (Exception e) {
			throw new BundleException("Unable to load bundle", e);
		}
	}

	public abstract RequiredComponents getRequiredComponents();

	@Override
	public Adapter createAdapterInstance() {
		loadExternalLibraries();
		return doCreateAdapterInstance();
	}

	protected abstract Adapter doCreateAdapterInstance();

}
