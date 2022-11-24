/**
 * 
 */
package org.crossroad.sdi.adapter.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;import org.crossroad.sdi.adapter.impl.UniqueNameTools;
import org.junit.jupiter.api.Test;

/**
 * @author e.soden
 *
 */
public class UniqueNameBuilderTest {
	@Test
	public void checkWithDefault() {
		try {
			String name = "\"catalog.schema.table\"";
			UniqueNameTools t = UniqueNameTools.build(name);
			assertNotNull("Catalog must not be null", t.getCatalog());
			assertEquals("catalog", t.getCatalog());
			assertEquals("schema", t.getSchema());
			assertEquals("table", t.getTable());
			
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}

	@Test
	public void checkWithLongName() {
		try {
			String name = "\"<none>.cmdb_rp.week\"";
			UniqueNameTools t = UniqueNameTools.build(name);
			assertNull("Catalog must be null", t.getCatalog());
			assertEquals("cmdb_rp", t.getSchema());
			assertEquals("week", t.getTable());
			
		} catch(Exception e)
		{
			fail(e.getMessage());
		}
	}

}
