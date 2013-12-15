package testing;


import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.Test;

import common.messages.KVMessage;

import app_kvClient.KVClient;
import app_kvClient.SocketStatus;

import junit.framework.TestCase;

/**
 * Test Cases for the Client application.
 * Please make sure the specified servers are running.
 * First Server: 127.0.0.1:50000
 * Second Server: 127.0.0.1:50001
 * Both servers must be running in order for tests to pass!
 * @author Elias Tatros
 *
 */
public class ClientTest extends TestCase {
	
	/* Address of first KVServer */
	String serverAddress = "127.0.0.1";
	int serverPort = 50000;
	
	/* Address of second KVServer */
	String server2Address = "127.0.0.1";
	int server2Port = 50001;
	
	/* Client Instance */
	KVClient client;
	
	/* Create new Client with metaData of 2 different servers */
	public void setUp() {
		
		Exception ex = null;
		
		try {
			client = new KVClient(); // client instance
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	@Test
	public void testConnectToServer() {
		try {
			// connect to first server
			client.connect(serverAddress, serverPort);
			// add the second server to the meta data
			client.getMetadata().addServer(server2Address + ":" + server2Port, server2Address, server2Port);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue(client.getConnectionStatus() == SocketStatus.CONNECTED);
	}
	
	@Test
	public void testPut2Servers() {
		Exception e = null;
		
		try {
			// connect to first server
			client.connect(serverAddress, serverPort);
			// artificially add the second server to the meta data
			client.getMetadata().addServer(server2Address + ":" + server2Port, server2Address, server2Port);
		} catch (UnknownHostException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		} catch (IOException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		
		try {
			assertTrue(client.getConnectionStatus() == SocketStatus.CONNECTED);
			
			// "A" will hash to value stored on first server (127.0.0.1:50000)
			client.put("A", "B");
			
			// "Z" will hash to value stored on second server (127.0.0.1:50001)
			client.put("Z", "Y");
			
			KVMessage receiveA = client.get("A"); // connects to 127.0.0.1:50000 and gets the value B
			KVMessage receiveZ = client.get("Z"); // connects to 127.0.0.1:50001 and gets the value Y
			
			// Verify Assertions
			assertTrue(receiveA.getValue().equals("B"));
			assertTrue(receiveZ.getValue().equals("Y"));
		} catch (Exception ex) {
			e = ex;
		}
		
		assertNull(e);
	}
}