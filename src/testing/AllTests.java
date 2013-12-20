package testing;


import org.junit.BeforeClass;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;


public class AllTests {
    private static boolean isRunning = false;
	  @BeforeClass
      public static void startServer() {
              if (!isRunning) {
                      KVServer.main(new String[]{"50000"});
                      isRunning = true;
              }
      }
	static {
		new KVServer(50000);
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		return clientSuite;
	}
	
}
