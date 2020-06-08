/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author e.soden
 *
 */
public class RequiredComponents {
	private Map<String[], Boolean> requiredCls = new HashMap<String[], Boolean>();

    private List<String> libPattern = new ArrayList<String>();


	/**
	 * 
	 */
	public RequiredComponents() {
		// TODO Auto-generated constructor stub
	}
	
	public void addClass(String[] clName, Boolean required) {
		this.requiredCls.put(clName, required);
	}

	/**
	 * @return
	 * @see java.util.Map#entrySet()
	 */
	public Set<Entry<String[], Boolean>> getRequiredClasses() {
		return requiredCls.entrySet();
	}
	
	public void addPatternLibrary(String libPattern) {
		this.libPattern.add(libPattern);
	}
	
	public List<String> getLibPattern() {
		return libPattern;
	}

}
