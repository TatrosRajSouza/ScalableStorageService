package testing;

import java.util.ArrayList;

import org.junit.Test;

import common.ServerData;
import app_kvEcs.ECS;
import junit.framework.TestCase;

public class ECSTests extends TestCase {
	private ECS ecs = new ECS();
	
	@Test
	public void testDefineServerRepositorySuccess() {
		Exception ex = null;
		boolean success = false;

		try {
			ecs.defineServerRepository("ecs.config");
		} catch (Exception e) {
			ex = e;
		}

		for (ServerData server : ecs.getServerRepository().getServers()) {
			if (server.getName().equals("node1")
					&& server.getAddress().equals("127.0.0.1")
					&& server.getPort() == 50000) {
				success = true;
			}
		}
		
		ecs.shutDown();
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testDefineServerRepositoryFail() {
		Exception ex = null;

		try {
			ecs.defineServerRepository("script.sh");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof IllegalArgumentException);
	}

	@Test
	public void testInitServiceAllNodes() {
		Exception ex = null;
		boolean success = true;
		ArrayList<ServerData> servers;

		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(8);
			servers = ecs.getStorageService().getServers();
			for (ServerData node : servers) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= servers.size() == 8;
			success &= ecs.getServerRepository().getServers().isEmpty();
		} catch (Exception e) {
			ex = e;
		}

		ecs.shutDown();
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testInitServiceSomeNodes() {
		Exception ex = null;
		boolean success = true;
		ArrayList<ServerData> servers;

		servers = ecs.getStorageService().getServers();
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(5);
			for (ServerData node : servers) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= servers.size() == 5;
			success &= ecs.getServerRepository().getServers().size() == 3;
		} catch (Exception e) {
			ex = e;
		}
		
		ecs.shutDown();
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testInitServiceMoreNodes() {
		Exception ex = null;
		boolean success = true;
		ArrayList<ServerData> servers;

		servers = ecs.getStorageService().getServers();
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(10);
			for (ServerData node : servers) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= servers.size() == 8;
			success &= ecs.getServerRepository().getServers().isEmpty();
		} catch (Exception e) {
			ex = e;
		}

		ecs.shutDown();
		assertNull(ex);
		assertTrue(success);
	}
	
	@Test
	public void testAddRemoveNode() {
		Exception ex = null;
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(4);
			ecs.start();
			ecs.addNode();
			ecs.addNode();
			ecs.addNode();
			ecs.addNode();
			ecs.addNode();
			ecs.removeNode();
			ecs.removeNode();
			ecs.addNode();
			ecs.addNode();
		} catch (Exception e) {
			ex = e;
		}
		
		ecs.shutDown();
		assertNull(ex);
	}

}
