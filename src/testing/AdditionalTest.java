package testing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

import app_kvEcs.ECSServerCommunicator;
import client.KVCommunication;
import client.KVStore;
import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.ServerData;
import junit.framework.TestCase;

/*        To the person that is going to grade this assignment.
 *        Please check the file QueryTest and KVDataTest.
 *        They are so many that we took off here and separated in way that is easier to understand
 */
public class AdditionalTest extends TestCase {

        @BeforeClass
        public static void testStartServer() {
                AllTests.startServer();
        }

        @Test
        public void testMultiplePut() throws UnknownHostException, IOException, InvalidMessageException {
        	ServerData server1Data = new ServerData("127.0.0.1:50000", "127.0.0.1", 50000);
			//ServerData server2Data = new ServerData("127.0.0.1:50001", "127.0.0.1", 50001);
			ArrayList<ServerData> serverListServer = new ArrayList<ServerData>();		
			serverListServer.add(server1Data);
			//serverListServer.add(server2Data);
			InfrastructureMetadata clientMetaData = new InfrastructureMetadata(serverListServer);
			ECSServerCommunicator server = new ECSServerCommunicator("127.0.0.1:50000", "127.0.0.1", 50000);
			ECSMessage initMessage = new ECSMessage(ECSStatusType.INIT, clientMetaData);
			KVCommunication comm  = new KVCommunication("127.0.0.1", 50000);
			comm.sendMessage(initMessage.toBytes());
			
			ECSMessage message = new ECSMessage(ECSStatusType.START);
			comm.sendMessage(message.toBytes());
			
                KVStore kvClient1 = new KVStore("localhost", 50000);
                kvClient1.connect();
                KVStore kvClient2 = new KVStore("localhost", 50000);
                kvClient2.connect();
                String key = "foobar";
                String value = "bar";
                String key1 = "foobar1";
                String value1 = "bar1";
                Exception ex = null;

                try {
                        kvClient1.put(key, value);
                        kvClient2.put(key1, value1);
                } catch (Exception e) {
                        ex = e;
                }

                assertTrue(ex == null );
                assertTrue(ex == null );
        }

       @Test
        public void testMultipleGet() throws UnknownHostException, IOException, InvalidMessageException {
    	   ServerData server1Data = new ServerData("127.0.0.1:50000", "127.0.0.1", 50000);
			//ServerData server2Data = new ServerData("127.0.0.1:50001", "127.0.0.1", 50001);
			ArrayList<ServerData> serverListServer = new ArrayList<ServerData>();		
			serverListServer.add(server1Data);
			//serverListServer.add(server2Data);
			InfrastructureMetadata clientMetaData = new InfrastructureMetadata(serverListServer);
			//ECSServerCommunicator server = new ECSServerCommunicator("127.0.0.1:50000", "127.0.0.1", 50000);
			ECSMessage initMessage = new ECSMessage(ECSStatusType.INIT, clientMetaData);
			KVCommunication comm  = new KVCommunication("127.0.0.1", 50000);
			comm.sendMessage(initMessage.toBytes());
			
			ECSMessage message = new ECSMessage(ECSStatusType.START);
			comm.sendMessage(message.toBytes());
                KVStore kvClient1 = new KVStore("localhost", 50000);
                kvClient1.connect();
                KVStore kvClient2 = new KVStore("localhost", 50000);
                kvClient2.connect();
                String key = "foobar";
                String value = "bar";


                String valuePut2 = null;
                KVMessage response2 = null;
                Exception ex = null;

                try {
                         kvClient1.put(key, value);
                        response2 = kvClient2.get(key);
                        valuePut2 = response2.getValue();

                } catch (Exception e) {
                        ex = e;
                }

                assertTrue(ex == null && valuePut2.equals("bar"));

        }
        @Test
        public void testMultipleUpdate() throws UnknownHostException, IOException, InvalidMessageException {
        	ServerData server1Data = new ServerData("127.0.0.1:50000", "127.0.0.1", 50000);
			//ServerData server2Data = new ServerData("127.0.0.1:50001", "127.0.0.1", 50001);
			ArrayList<ServerData> serverListServer = new ArrayList<ServerData>();		
			serverListServer.add(server1Data);
			//serverListServer.add(server2Data);
			InfrastructureMetadata clientMetaData = new InfrastructureMetadata(serverListServer);
			//ECSServerCommunicator server = new ECSServerCommunicator("127.0.0.1:50000", "127.0.0.1", 50000);
			ECSMessage initMessage = new ECSMessage(ECSStatusType.INIT, clientMetaData);
		KVCommunication comm  = new KVCommunication("127.0.0.1", 50000);
			comm.sendMessage(initMessage.toBytes());
			
			ECSMessage message = new ECSMessage(ECSStatusType.START);
			comm.sendMessage(message.toBytes());
                KVStore kvClient1 = new KVStore("localhost", 50000);
                kvClient1.connect();
                KVStore kvClient2 = new KVStore("localhost", 50000);
                kvClient2.connect();
                String key = "foobar";
                String value = "bar";


                String valuePut2 = null;
                KVMessage response2 = null;
                Exception ex = null;

                try {
                         kvClient1.put(key, value);
                         String value1 = "bar1";
                        kvClient2.put(key, value1 );
                        response2 = kvClient1.get(key);
                        valuePut2 = response2.getValue();

                } catch (Exception e) {
                        ex = e;
                }

                assertTrue(ex == null && valuePut2.equals("bar1"));

        }
        @Test
        public void testStub() {
                assertTrue(true);
        }
}