package testing;

import java.math.BigInteger;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.junit.Test;

import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;

public class ECSMessageTest extends TestCase {
	@BeforeClass
	public void testSetEncoding() {
		System.setProperty("file.encoding", "US-ASCII");
	}

	@Test
	public void testOneArgumentMessageSuccess() {
		ECSMessage ecsMsg1 = null,ecsMsg2 = null;
		Exception ex = null;
		ECSStatusType command0, command1 = null, command2 = null;
		byte[] bytes0, bytes1 = null, bytes2 = null;

		command0 = ECSStatusType.START;
		bytes0 = "START\r".getBytes();
		try {
			ecsMsg1 = new ECSMessage(command0);
			command1 = ecsMsg1.getCommand();
			bytes1 = ecsMsg1.toBytes();
			ecsMsg2 = new ECSMessage(bytes1);
			command2 = ecsMsg2.getCommand();
			bytes2 = ecsMsg2.toBytes();
		} catch (InvalidMessageException e) {
			ex = e;
		}

		assertEquals(command0, command1);
		assertEquals(command2, command1);
		assertEquals(new String(bytes0), new String(bytes1));
		assertEquals(new String(bytes2), new String(bytes1));
		assertNull(ex);
	}

	@Test
	public void testTwoArgumentMessageSuccess() {
		ECSMessage ecsMsg1 = null, ecsMsg2 = null;
		Exception ex = null;
		ECSStatusType command0, command1 = null, command2 = null;
		InfrastructureMetadata metadata0, metadata1 = null, metadata2 = null;
		byte[] bytes0, bytes1 = null, bytes2 = null;

		command0 = ECSStatusType.INIT;
		metadata0 = new InfrastructureMetadata();
		metadata0.addServer("node1", "127.0.0.1", 50000);
		metadata0.addServer("node2", "127.0.0.1", 50001);
		metadata0.addServer("node3", "127.0.0.1", 50002);
		bytes0 = "INIT\nnode1,127.0.0.1,50000;node2,127.0.0.1,50001;node3,127.0.0.1,50002;\r".getBytes();
		
		try {
			ecsMsg1 = new ECSMessage(command0, metadata0);
			command1 = ecsMsg1.getCommand();
			metadata1 = ecsMsg1.getMetadata();
			bytes1 = ecsMsg1.toBytes();
			ecsMsg2 = new ECSMessage(bytes1);
			command2 = ecsMsg2.getCommand();
			metadata2 = ecsMsg1.getMetadata();
			bytes2 = ecsMsg2.toBytes();
		} catch (InvalidMessageException e) {
			ex = e;
		}

		assertEquals(command0, command1);
		assertEquals(command2, command1);
		assertEquals(metadata0.toString(), metadata1.toString());
		assertEquals(metadata2.toString(), metadata1.toString());
		assertEquals(new String(bytes0), new String(bytes1));
		assertEquals(new String(bytes2), new String(bytes1));
		assertNull(ex);
	}
	
	@Test
	public void testFourArgumentMessageSuccess() {
		ECSMessage ecsMsg1 = null, ecsMsg2 = null;
		Exception ex = null;
		ECSStatusType command0, command1 = null, command2 = null;
		BigInteger start0, start1 = null, start2 = null;
		ServerData server0, server1 = null, server2 = null; 
		byte[] bytes0, bytes1 = null, bytes2 = null;

		command0 = ECSStatusType.MOVE_DATA;
		start0 = BigInteger.valueOf(10);
		server0 = new ServerData("node1", "127.0.0.1", 50000);
		bytes0 = ("MOVE_DATA\n10\nnode1\n127.0.0.1\n50000\r").getBytes();
		
		try {
			ecsMsg1 = new ECSMessage(command0, start0, server0);
			command1 = ecsMsg1.getCommand();
			start1 = ecsMsg1.getStartIndex();
			server1 = ecsMsg1.getServer();
			bytes1 = ecsMsg1.toBytes();
			ecsMsg2 = new ECSMessage(bytes1);
			command2 = ecsMsg2.getCommand();
			start2 = ecsMsg2.getStartIndex();
			server2 = ecsMsg2.getServer();
			bytes2 = ecsMsg2.toBytes();
		} catch (InvalidMessageException e) {
			ex = e;
		}

		assertEquals(command0, command1);
		assertEquals(command2, command1);
		assertEquals(start0.toString(), start1.toString());
		assertEquals(start2.toString(), start1.toString());
		assertEquals(server0.getName() + server0.getAddress() + server0.getPort(),
				server1.getName() + server1.getAddress() + server1.getPort());
		assertEquals(server2.getName() + server2.getAddress() + server2.getPort(),
				server1.getName() + server1.getAddress() + server1.getPort());
		assertEquals(new String(bytes0), new String(bytes1));
		assertEquals(new String(bytes2), new String(bytes1));
		assertNull(ex);
	}
	
	@Test
	public void testMessageFail() {
		ECSMessage ecsMsg1 = null;
		Exception ex = null;

		try {
			ecsMsg1 = new ECSMessage(ECSStatusType.SHUTDOWN);
			ecsMsg1.getStartIndex();
		} catch (InvalidMessageException e) {
			ex = e;
		}

		assertTrue(ex instanceof InvalidMessageException);
	}
}
