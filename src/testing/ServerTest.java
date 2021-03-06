package testing;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.Test;

import common.InfrastructureMetadata;
import common.ServerData;
import common.messages.InvalidMessageException;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import junit.framework.TestCase;

/**
 * @author Elias Tatros
 *
 */
public class ServerTest extends TestCase {
	
	/* Address of first KVServer */
	String serverAddress = "127.0.0.1";
	int serverPort = 50000;
	
	/* Address of second KVServer */
	String server2Address = "127.0.0.1";
	int server2Port = 50001;
	
	/* Client Instance */
	KVClient client;
	KVServer server1;
	KVServer server2;
	
	boolean onlyOnce = false;
	
	/* Create new Client with metaData of 2 different servers */
	public void setUp() {
		
		if (!onlyOnce) {
			onlyOnce = true;
			Exception ex = null;
			
			try {
				server1 = new KVServer(serverPort);
				server1.start();
				
				server2 = new KVServer(server2Port);
				server2.start();
				
				client = new KVClient(); // client instance
			} catch (Exception e) {
				ex = e;
				e.printStackTrace();
			}	
			
			assertNull(ex);
		}
	}
	
	@Test
	public void testEverything() {
		Exception e = null;
		try {
			ServerData server1Data = new ServerData("127.0.0.1:50000", "127.0.0.1", 50000);
			ServerData server2Data = new ServerData("127.0.0.1:50001", "127.0.0.1", 50001);
			ArrayList<ServerData> serverListServer = new ArrayList<ServerData>();
			serverListServer.add(server1Data);
			serverListServer.add(server2Data);
			
			String key1 = "A";
			String value1 = "B";
			
			client.connect(serverAddress, serverPort);
			
			server1.setServeClientRequest(true);
			server2.setServeClientRequest(true);
			server1.setMetaData(new InfrastructureMetadata(serverListServer));
			server2.setMetaData(new InfrastructureMetadata(serverListServer));
			
			client.put(key1, value1);
			// String value = client.get(key1).getValue();
		} catch (UnknownHostException ex) {
			e = ex;
			System.out.println("Unknown Host!");
		} catch (ConnectException ex) {
			e = ex;
			System.out.println("Could not establish connection! Reason: " + ex.getMessage());
		} catch (IOException ex) {
			e = ex;
			System.out.println("Could not establish connection! IOException");
		} catch (InvalidMessageException ex) {
			e = ex;
			System.out.println("Unable to connect to server. Received an invalid message: \n" + ex.getMessage());
		}
		
		/*
		for (int i = 0; i < 100; i++)
		{
			ServerData server1 = new ServerData("127.0.0.1:50000", "127.0.0.1", 50000);
			ServerData server2 = new ServerData("127.0.0.1:50001", "127.0.0.1", 50001);
			ArrayList<ServerData> serverList = new ArrayList<ServerData>();
			serverList.add(server1);
			serverList.add(server2);
			
			ConsistentHashing consHash = new ConsistentHashing(serverList);
			
			try {
				System.out.println(consHash.getServerForKey("A").getPort());
			} catch (IllegalArgumentException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (EmptyServerDataException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		*/
		assertNull(e);
	}
}