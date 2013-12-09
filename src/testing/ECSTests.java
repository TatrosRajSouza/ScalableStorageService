package testing;

import org.junit.Test;

import app_kvEcs.ECS;
import app_kvEcs.KVStorageServer;
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
		
		for (KVStorageServer server : ecs.getServerRepository()) {
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
}
