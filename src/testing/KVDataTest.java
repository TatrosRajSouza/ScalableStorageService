package testing;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import app_kvServer.KVData;

public class KVDataTest {

	KVData kvData = new KVData();

	@Test
	public void testPut() {
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		String returnValue1 = kvData.put(b1,"jona");
		assertTrue( returnValue1 == null);
		String returnValue2 = kvData.put(b2,"gull");
		assertTrue(returnValue2 == null);
	}

	@Test
	public void testGet() {
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		kvData.put(b1,"jona");
		kvData.put(b2,"gull");
		assertTrue(kvData.get(b1).equals("jona"));
		assertTrue(kvData.get(b2).equals("gull"));
	}

	@Test
	public void testUpdate() {
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		kvData.put(b1,"jona");
		kvData.put(b2,"gull");
		String returnValue = kvData.put(b2,"seagull");
		assertTrue( returnValue.equals("seagull"));
		assertTrue(kvData.get(b2).equals("seagull"));
	}
	@Test
	public void testDelete() {
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		kvData.put(b1,"jona");
		kvData.put(b2,"gull");
		kvData.put(b2,"seagull");
		String returnValue = kvData.put(b2,"null");
		assertTrue( returnValue.equals("seagull"));
		String getValue = kvData.get(b2);
		assertTrue(getValue == null);
	}
	@Test
	public void testFindMovingData()
	{
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		BigInteger b3 = new BigInteger("3");
		BigInteger b4 = new BigInteger("4");
		BigInteger b5 = new BigInteger("5");
		BigInteger b6 = new BigInteger("6");
		kvData.put(b1,"jona1");
		kvData.put(b2,"jona2");
		kvData.put(b3,"jona3");
		kvData.put(b4,"jona4");
		kvData.put(b5,"jona5");
		kvData.put(b6,"jona6");
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		movingData = kvData.findMovingData(b3, b5);
		assertTrue(movingData.size() == 3);
		Iterator<Entry<BigInteger, String>> it = movingData.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<BigInteger,String> pairs = (Entry<BigInteger, String>)it.next();
			BigInteger key = pairs.getKey();
			if(key.equals(b3))
				assertTrue(pairs.getValue().equals("jona3"));
			if(key.equals(b4))
					assertTrue(pairs.getValue().equals("jona4"));
			if(key.equals(b5))
					assertTrue(pairs.getValue().equals("jona5"));
		}
		movingData = kvData.findMovingData(b5, b3);
		assertTrue(movingData.size() == 3);
		Iterator<Entry<BigInteger, String>> it1 = movingData.entrySet().iterator();
		while (it1.hasNext()) {
			Map.Entry<BigInteger,String> pairs = (Entry<BigInteger, String>)it1.next();
			BigInteger key = pairs.getKey();
			if(key.equals(b3))
				assertTrue(pairs.getValue().equals("jona3"));
			if(key.equals(b4))
					assertTrue(pairs.getValue().equals("jona4"));
			if(key.equals(b5))
					assertTrue(pairs.getValue().equals("jona5"));
		}

	}
	
	@Test
	public void testMovedatatoEmpty()
	{
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		BigInteger b3 = new BigInteger("3");
		BigInteger b4 = new BigInteger("4");
		BigInteger b5 = new BigInteger("5");
		BigInteger b6 = new BigInteger("6");
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		movingData.put(b1,"jona1");
		movingData.put(b2,"jona2");
		movingData.put(b3,"jona3");
		movingData.put(b4,"jona4");
		movingData.put(b5,"jona5");
		movingData.put(b6,"jona6");
		kvData.moveData(movingData);
		ConcurrentHashMap<BigInteger, String> dataStore = kvData.dataStore;
		assertTrue(kvData.dataStore.size() == 6);
		
	}
	@Test
	public void testMovedatatonotEmpty()
	{
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		BigInteger b3 = new BigInteger("3");
		BigInteger b4 = new BigInteger("4");
		BigInteger b5 = new BigInteger("5");
		BigInteger b6 = new BigInteger("6");
		kvData.put(b1,"jona1");
		kvData.put(b2,"jona2");
		kvData.put(b3,"jona3");
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		movingData.put(b4,"jona4");
		movingData.put(b5,"jona5");
		movingData.put(b6,"jona6");
		kvData.moveData(movingData);
		ConcurrentHashMap<BigInteger, String> dataStore = kvData.dataStore;
		assertTrue(kvData.dataStore.size() == 6);
	}
	@Test
	public void testRemoveData()
	{
		BigInteger b1 = new BigInteger("1");
		BigInteger b2 = new BigInteger("2");
		BigInteger b3 = new BigInteger("3");
		BigInteger b4 = new BigInteger("4");
		BigInteger b5 = new BigInteger("5");
		BigInteger b6 = new BigInteger("6");
		kvData.put(b1,"jona1");
		kvData.put(b2,"jona2");
		kvData.put(b3,"jona3");
		kvData.put(b4,"jona4");
		kvData.put(b5,"jona5");
		kvData.put(b6,"jona6");
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		movingData.put(b4,"jona4");
		movingData.put(b5,"jona5");
		movingData.put(b6,"jona6");
		kvData.remove(movingData);
		ConcurrentHashMap<BigInteger, String> dataStore = kvData.dataStore;
		assertTrue(kvData.dataStore.size() == 3);
	}
}
