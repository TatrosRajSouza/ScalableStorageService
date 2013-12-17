package testing;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import common.messages.InfrastructureMetadata;
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
	
	boolean onlyOnce = false;
	
	/* Create new Client with metaData of 2 different servers */
	public void setUp() {
		
		if (!onlyOnce) {
			onlyOnce = true;
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
				server1.start();
				
				server2 = new KVServer(server2Port);
				server2.start();
				
				client = new KVClient(); // client instance
			} catch (Exception e) {
				ex = e;
			}	
			
			assertNull(ex);
		}
	}
	
	@Test
	public void testEverything() {
		Exception e = null;
		
		try {
			client.connect(serverAddress, serverPort);
			client.disconnect();
			client.connect(server2Address, server2Port);
			client.disconnect();
			
			client.connect(serverAddress, serverPort);
			server1.setServeClientRequest(true);
			server2.setServeClientRequest(true);
			server1.setMetaData(new InfrastructureMetadata(client.getMetadata().getServers()));
			server2.setMetaData(new InfrastructureMetadata(client.getMetadata().getServers()));
			client.put("A", "B");
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
}