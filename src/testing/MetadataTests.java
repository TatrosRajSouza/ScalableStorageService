package testing;

import org.junit.Test;
import common.messages.InfrastructureMetadata;
import junit.framework.TestCase;

public class MetadataTests extends TestCase {
	InfrastructureMetadata metaData;
	String initialDataStr = "ServerA,127.0.0.1,50000;ServerB,127.0.0.2,50001;ServerC,127.0.0.3,50002;ServerD,127.0.0.4,50003;";
	
	public void setUp() {
		
		Exception ex = null;
		
		try {
			metaData = new InfrastructureMetadata(initialDataStr);
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	@Test
	public void testStringRepresentation() {	
		assertTrue(metaData.toString().equals(initialDataStr));
	}
	
	@Test
	public void testExpandMetaData() {
		metaData.addServer("ServerE", "127.0.0.5", 50004);
		metaData.addServer("ServerF", "127.0.0.6", 50005);
		String newDataStr = initialDataStr + "ServerE,127.0.0.5,50004;ServerF,127.0.0.6,50005;";
		
		assertTrue(metaData.toString().equals(newDataStr));
	}
	
	@Test
	public void testRemoveMetaData() {
		metaData.removeServer("127.0.0.5", 50004);
		metaData.removeServer("127.0.0.6", 50005);
		
		assertTrue(metaData.toString().equals(initialDataStr));
	}
}
