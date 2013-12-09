package testing;

import java.util.ArrayList;
import java.util.SortedMap;

import org.junit.Test;

import client.ConsistentHashing;
import common.messages.ServerData;
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
		
		assertTrue(map.containsValue("127.0.0.255:50000"));
		assertTrue(map.containsValue("127.0.0.254:50001"));
		assertTrue(map.containsValue("127.0.0.253:50002"));
		assertTrue(map.containsValue("127.0.0.252:50003"));
	}
}