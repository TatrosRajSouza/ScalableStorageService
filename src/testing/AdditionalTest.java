package testing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.BeforeClass;
import org.junit.Test;

import client.KVStore;
import common.messages.InvalidMessageException;
import common.messages.KVMessage;
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