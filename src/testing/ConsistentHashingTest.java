package testing;

import java.util.ArrayList;
import java.util.SortedMap;

import org.junit.Test;

import common.messages.ServerData;
import consistent_hashing.ConsistentHashing;
import junit.framework.TestCase;

public class ConsistentHashingTest extends TestCase {
	// tests operate on this instance of metaData
	ConsistentHashing conHash;
	
	/* Create initial metaData used in tests */
	public void setUp() {
		
		Exception ex = null;
		
		try {
			conHash = new ConsistentHashing();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	@Test
	/* Add certain servers to metaData */
	public void testHashCircleFromServerData() {
		ArrayList<ServerData> servers = new ArrayList<ServerData>();
		servers.add(new ServerData("ServerA","127.0.0.1",50000));
		servers.add(new ServerData("ServerB","127.0.0.2",50001));
		servers.add(new ServerData("ServerC","127.0.0.3",50002));
		servers.add(new ServerData("ServerD","127.0.0.4",50003));
		servers.add(new ServerData("ServerE","127.0.0.5",50004));
		
		conHash = new ConsistentHashing(servers);
		
		SortedMap<Integer, String> map = conHash.getHashCircle();
		
		assertTrue(map.containsValue("127.0.0.1:50000"));
		assertTrue(map.containsValue("127.0.0.2:50001"));
		assertTrue(map.containsValue("127.0.0.3:50002"));
		assertTrue(map.containsValue("127.0.0.4:50003"));
		assertTrue(map.containsValue("127.0.0.5:50004"));
	}
	
	@Test
	/* Add certain servers to metaData */
	public void testAddServers() {
		conHash.addServer("127.0.0.255", 50000);
		conHash.addServer("127.0.0.254", 50001);
		conHash.addServer("127.0.0.253", 50002);
		conHash.addServer("127.0.0.252", 50003);
		
		SortedMap<Integer, String> map = conHash.getHashCircle();
		// {-1067817501=127.0.0.254:50001, 918589990=127.0.0.253:50002, 2002620904=127.0.0.252:50003, 2012606280=127.0.0.255:50000}
		
		assertTrue(map.containsValue("127.0.0.255:50000"));
		assertTrue(map.containsValue("127.0.0.254:50001"));
		assertTrue(map.containsValue("127.0.0.253:50002"));
		assertTrue(map.containsValue("127.0.0.252:50003"));
	}
	
	public void testLocateServerForKey() {
													
		String key1 = "ThisIsAKey"; // keyHash: 1969410312      -> Expected Server: 127.0.0.252:50003 (hash: 2002620904)
		String key2 = "ExampleKey"; // keyHash: -1480982169		-> Expected Server: 127.0.0.254:50001 (hash: -1067817501)
		Exception e = null;
		
		// Map: {-1067817501=127.0.0.254:50001, 918589990=127.0.0.253:50002, 2002620904=127.0.0.252:50003, 2012606280=127.0.0.255:50000}
		conHash.addServer("127.0.0.255", 50000);
		conHash.addServer("127.0.0.254", 50001);
		conHash.addServer("127.0.0.253", 50002);
		conHash.addServer("127.0.0.252", 50003);
		
		try {
			ServerData serverDataKey1 = conHash.getServerForKey(key1); 
			// System.out.println("Name: " + serverDataKey1.getName() + "\nAddress: " + serverDataKey1.getAddress() + "\nPort: " + serverDataKey1.getPort());
			
			ServerData serverDataKey2 = conHash.getServerForKey(key2);
			// System.out.println("Name: " + serverDataKey2.getName() + "\nAddress: " + serverDataKey2.getAddress() + "\nPort: " + serverDataKey2.getPort());
			
			assertTrue(serverDataKey1.getName().equals("127.0.0.252:50003"));
			assertTrue(serverDataKey2.getName().equals("127.0.0.254:50001"));
		} catch (Exception ex) {
			e = ex;
		}
		
		assertNull(e);
	}
}