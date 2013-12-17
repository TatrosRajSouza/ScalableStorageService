package testing;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.junit.Test;

import common.messages.InvalidMessageException;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;

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
	
	/* Create new Client with metaData of 2 different servers */
	public void setUp() {
		Exception ex = null;
		
		try {
			new LogSetup("logs/client.log", Level.ALL);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			ex = e1;
		}
		
		try {
			server1 = new KVServer(serverPort);
			server2 = new KVServer(server2Port);
			client = new KVClient(); // client instance
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	@Test
	public void testConnectDisconnectClient() {
		Exception e = null;
		
		server1.serveClientRequest = true;
		server2.serveClientRequest = true;
		System.out.println("Server ready");
		
		try {
			client.connect(serverAddress, serverPort);
			client.disconnect();
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
		
		assertNull(e);
	}
	
	/*
	@Test
	public void testPUT() {
		Exception e = null;
		
		try {
			client.put("A", "B");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	*/
}