package testing;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import app_kvEcs.ECS;
import app_kvEcs.KVServerNode;
import junit.framework.TestCase;

public class ECSTests extends TestCase {
	private ECS ecs;

	@Test
	public void testDefineServerRepositorySuccess() {
		Exception ex = null;
		boolean success = false;

		ecs = new ECS();
		try {
			ecs.defineServerRepository("ecs.config");
		} catch (Exception e) {
			ex = e;
		}

		for (KVServerNode server : ecs.getServerRepository()) {
			if (server.getName().equals("node1")
					&& server.getAddress().equals("127.0.0.1")
					&& server.getPort() == 50000) {
				success = true;
			}
		}
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testDefineServerRepositoryFail() {
		Exception ex = null;

		ecs = new ECS();
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

		ecs = new ECS();
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(8);
			for (KVServerNode node : ecs.getStorageService()) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= (ecs.getStorageService().size() == 8);
			success &= ecs.getServerRepository().isEmpty();
		} catch (Exception e) {
			ex = e;
		}

		//ecs.printHashRing();
		System.out.println();
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testInitServiceSomeNodes() {
		Exception ex = null;
		boolean success = true;

		ecs = new ECS();
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(5);
			for (KVServerNode node : ecs.getStorageService()) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= (ecs.getStorageService().size() == 5);
			success &= (ecs.getServerRepository().size() == 3);
		} catch (Exception e) {
			ex = e;
		}
		//ecs.printHashRing();
		//System.out.println();
		assertNull(ex);
		assertTrue(success);
	}

	@Test
	public void testInitServiceMoreNodes() {
		Exception ex = null;
		boolean success = true;

		ecs = new ECS();
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(10);
			for (KVServerNode node : ecs.getStorageService()) {
				success &= node.getName().equals("node1") ||
						node.getName().equals("node2")    ||
						node.getName().equals("node3")    ||
						node.getName().equals("node4")    ||
						node.getName().equals("node5")    ||
						node.getName().equals("node6")    ||
						node.getName().equals("node7")    ||
						node.getName().equals("node8");
			}
			success &= (ecs.getStorageService().size() == 8);
			success &= ecs.getServerRepository().isEmpty();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(success);
	}
	
	@Test
	public void testEndIndexHash() {
		Exception ex = null;
		Set<BigInteger> endIndexes = new HashSet<BigInteger>();
		ecs = new ECS();
		
		try {
			ecs.defineServerRepository("ecs.config");
			ecs.initService(8);
			
			for (KVServerNode node : ecs.getStorageService()) {
				endIndexes.add(node.getNextNode().getStartIndex());
			}
			ecs.printHashRing1();
			assertEquals(endIndexes.size(), 8);
		} catch (Exception e) {
			ex = e;
		}
		
		assertNull(ex);
	}
}
