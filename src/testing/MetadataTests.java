package testing;

import java.util.ArrayList;

import org.junit.Test;

import common.InfrastructureMetadata;
import common.ServerData;
import junit.framework.TestCase;

public class MetadataTests extends TestCase {
	// tests operate on this instance of metaData
	InfrastructureMetadata metaData;
	// String representation of server infrastructure used to create initial metaData
	String initialDataStr = "ServerA,127.0.0.1,50000;ServerB,127.0.0.2,50001;ServerC,127.0.0.3,50002;ServerD,127.0.0.4,50003;";
	
	/* Create initial metaData used in tests */
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
	/* Obtain String representation used in other functions (e.g. update / getBytes) */
	public void testStringRepresentation() {	
		assertTrue(metaData.toString().equals(initialDataStr));
	}
	
	@Test
	/* Add certain servers to metaData */
	public void testExpandMetaData() {
		metaData.addServer("ServerE", "127.0.0.5", 50004);
		metaData.addServer("ServerF", "127.0.0.6", 50005);
		String newDataStr = initialDataStr + "ServerE,127.0.0.5,50004;ServerF,127.0.0.6,50005;";
		
		assertTrue(metaData.toString().equals(newDataStr));
	}
	
	@Test
	/* Remove certain servers from metaData */
	public void testRemoveMetaData() {
		metaData.addServer("ServerE", "127.0.0.5", 50004);
		metaData.addServer("ServerF", "127.0.0.6", 50005);
		metaData.removeServer("127.0.0.5", 50004);
		metaData.removeServer("127.0.0.6", 50005);
		
		assertTrue(metaData.toString().equals(initialDataStr));
	}
	
	@Test
	/* Completely replace metaData with new version */
	public void testUpdateMetaData() {
		/* A different way to create Server data
		 * Create Data objects for individual Servers, then add them to a list, 
		 * then pass the list to the InfrastructureMetadata constructor
		 */
		ArrayList<ServerData> servers = new ArrayList<ServerData>();
		servers.add(new ServerData("ServerZ","127.0.0.255",50000));
		servers.add(new ServerData("ServerY","127.0.0.254",50001));
		servers.add(new ServerData("ServerX","127.0.0.253",50002));
		
		/* Create new meta data from server list */
		InfrastructureMetadata newMetaData = new InfrastructureMetadata(servers);
		
		/* Update initial metaData to this new version using the String representation */
		metaData.update(newMetaData.toString());
		
		assertTrue(metaData.toString().equals(newMetaData.toString()));
	}
}