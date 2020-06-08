/**
 * 
 */
package org.crossroad.sdi.adapter.impl;

import java.util.HashMap;
import java.util.Map;

import com.sap.hana.dp.adapter.sdk.Column;

/**
 * @author e.soden
 *
 */
public class ColumnHelper {

	public interface IColdef {
		public int getLenght();

		public int getScale();

		public int getPrecision();

		public int getNativeTypeID();
	}

	private class ColumnDef implements IColdef {
		private int length = 0;
		private int scale = 0;
		private int precision = 0;
		private int nativeTypeID = 0;

		public ColumnDef() {
		}

		@Override
		public int getLenght() {
			return length;
		}

		@Override
		public int getScale() {
			return scale;
		}

		@Override
		public int getPrecision() {
			return precision;
		}

		@Override
		public int getNativeTypeID() {
			return nativeTypeID;
		}
	}

	private Map<String, IColdef> container = new HashMap<String, ColumnHelper.IColdef>();

	/**
	 * 
	 */
	public ColumnHelper() {

	}

	/*
	 * 
	 */
	public void addColumn(Column col, int dbtype) {
		ColumnDef def = new ColumnDef();

		def.length = col.getLength();
		def.scale = col.getScale();
		def.precision = col.getPrecision();
		def.nativeTypeID = dbtype;
		container.put(col.getName(), def);
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public IColdef getColumn(String name) {
		return container.get(name);
	}

	public void clear() {
		container.clear();
	}
}
