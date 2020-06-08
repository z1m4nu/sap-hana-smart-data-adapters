/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import com.sap.hana.dp.adapter.sdk.CredentialEntry;
import com.sap.hana.dp.adapter.sdk.CredentialProperties;
import com.sap.hana.dp.adapter.sdk.PropertyEntry;
import com.sap.hana.dp.adapter.sdk.PropertyGroup;

/**
 * @author e.soden
 *
 */
public final class RemoteSourceDescriptionFactory {

	/**
	 * 
	 */
	private RemoteSourceDescriptionFactory() {
		// TODO Auto-generated constructor stub
	}
	
	public static PropertyEntry getOptionNullableAsEmpty() {
		PropertyEntry entry = new PropertyEntry(AdapterConstants.KEY_NULLASEMPTYSTRING, "NULL as empty data",
				"Allow to show null data as empty", false);
		entry.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		entry.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		entry.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);
		
		return entry;
	}
	
	public static PropertyEntry getOptionDisplaySystemTables() {
		PropertyEntry entry = new PropertyEntry(AdapterConstants.KEY_WITHSYS, "Display system data",
				"Allow to show or not system data", false);
		entry.addChoice(AdapterConstants.BOOLEAN_TRUE, AdapterConstants.BOOLEAN_TRUE);
		entry.addChoice(AdapterConstants.BOOLEAN_FALSE, AdapterConstants.BOOLEAN_FALSE);
		entry.setDefaultValue(AdapterConstants.BOOLEAN_FALSE);
		
		return entry;
	}

	/**
	 * 
	 * @return
	 */
	public static PropertyGroup getBasicJDBCConnectionGroup() {
		PropertyGroup connectGroup = new PropertyGroup(AdapterConstants.KEY_GROUP_CONNECTION, "JDBC connection");
		PropertyEntry hostEntry = new PropertyEntry(AdapterConstants.KEY_HOSTNAME, "Hostname", "Database Hostname", true);
		PropertyEntry portEntry = new PropertyEntry(AdapterConstants.KEY_PORT, "TCP Port", "Database listen port", true);
		PropertyEntry dbsEntry = new PropertyEntry(AdapterConstants.KEY_DATABASE, "Database name", "Database name", true);
		PropertyEntry optionEntry = new PropertyEntry(AdapterConstants.KEY_OPTION, "Connection option", "Set JDBC Url optional", false);
		

	
		
		
		
		connectGroup.addProperty(hostEntry);
		connectGroup.addProperty(portEntry);
		connectGroup.addProperty(dbsEntry);
		connectGroup.addProperty(optionEntry);
		connectGroup.addProperty(getOptionDisplaySystemTables());
		connectGroup.addProperty(getOptionNullableAsEmpty());

		return connectGroup;
	}
	
	
	public static PropertyGroup getJDBCConnectionGroup()
	{
		PropertyGroup group = new PropertyGroup(AdapterConstants.KEY_GROUP_CONNECTION, "Expert mode");
		PropertyEntry jdbcurl = new PropertyEntry(AdapterConstants.KEY_JDBC_URL, "URL",
				"The URL of the connection, e.g. jdbc:sqlserver://localhost;databaseName=master");

		PropertyEntry jdbcDrv = new PropertyEntry(AdapterConstants.KEY_JDBC_DRIVERCLASS, "Driver Class",
				"The class name to use, e.g. com.microsoft.sqlserver.jdbc.SQLServerDriver");

		PropertyEntry jdbcJar = new PropertyEntry(AdapterConstants.KEY_JDBC_JAR, "JDBC Driver jar file",
				"the location of the jdbc driver's jar file on the agent computer, e.g. lib/sqljdbc.jar");
		
		group.addProperty(jdbcurl);
		group.addProperty(jdbcDrv);
		group.addProperty(jdbcJar);

		
		
		return group;
	}
	
	/**
	 * 
	 * @return
	 */
	public static CredentialProperties getCredentialProperties() {
		CredentialProperties credentialProperties = new CredentialProperties();
		CredentialEntry credential = new CredentialEntry("credential", "JDBC Credentials");
		credential.getUser().setDisplayName("Username");
		credential.getPassword().setDisplayName("Password");
		
		credentialProperties.addCredentialEntry(credential);

		return credentialProperties;
	}
	
	
}
