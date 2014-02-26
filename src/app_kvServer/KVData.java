package app_kvServer;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import consistent_hashing.ConsistentHashing;

/**
 * The KVData stores data which is sent by clients in a key -> value fashion . 
 * @author Udhayaraj Sivalingam
 */

public class KVData {
	public ConcurrentHashMap<BigInteger, String> dataStore = new ConcurrentHashMap<BigInteger, String>();
	private Logger logger;
	
	public KVData()
	{
		LogSetup ls = new LogSetup("logs/KVDATA.log", "KVDATA", Level.ALL);
		this.logger = ls.getLogger();
	}

	public KVData(String data) {
		String[] dataArray = data.split("###");
		for (String dataEntry : dataArray) {
			String[] dataEntryValues = dataEntry.split("$$$");
			dataStore.put(ConsistentHashing.hashKey(dataEntryValues[0]), dataEntryValues[0]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder data = new StringBuilder();
		for (BigInteger key : dataStore.keySet()) {
			data.append(key.toString() + "$$$" + dataStore.get(key) + "###");
		}
		
		return data.toString();
	}
	/**
	 * store/update/delete key,value pair in KVdata .
	 */
	public String put(BigInteger key, String value) {
		String returnValue = null;
		if(!value.equals("null"))
		{
			returnValue = dataStore.putIfAbsent(key, value);
			if(returnValue != null)
			{

				if(dataStore.replace(key, value)!= null)
					returnValue = dataStore.get(key);
			}
		}
		else
		{

			returnValue = dataStore.remove(key);

		}
		return returnValue;

	}
	/**
	 * move data from one kvdata to other.
	 */
	public void moveData(HashMap<BigInteger,String> movingData)
	{
		if(!movingData.isEmpty())
		{
		if(!dataStore.isEmpty())
		{
			Iterator<Entry<BigInteger, String>> it = movingData.entrySet().iterator();
			while (it.hasNext()) {
				Entry<BigInteger, String> pairs = (Map.Entry<BigInteger,String>)it.next();
				dataStore.put(pairs.getKey(), pairs.getValue());
			}
		}
		else
			dataStore.putAll(movingData);
		}

	}
	
	/**
	 * get value for given key.
	 */
	public String get(BigInteger hashedKey)  {
		return dataStore.get(hashedKey);
	}
	
	/**
	 * Find data set which is to be moved from one server to other.
	 */
	public HashMap<BigInteger,String> findMovingData(BigInteger startIndex, BigInteger endIndex,boolean corner)
	{
		BigInteger key;
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		if(!dataStore.isEmpty())
		{
		Iterator<Entry<BigInteger, String>> it = dataStore.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<BigInteger,String> pairs = (Entry<BigInteger, String>)it.next();
			key = pairs.getKey();
			if(!corner)
			{
			if((key.compareTo(startIndex)  >= 0  && key.compareTo(endIndex) <= 0 ))
			{
				logger.info("correct1");
				movingData.put(key, pairs.getValue());
			}
			}
			else
			{
			 if((key.compareTo(startIndex)  >= 0  &&  key.compareTo(endIndex) >= 0 )||(key.compareTo(startIndex)  <= 0  &&  key.compareTo(endIndex) <= 0 ))
			{
				logger.info("correct2");
				movingData.put(key, pairs.getValue());
			}
			}
		}
		}
		return movingData;
	}
	/**
	 * remove data set from server.
	 */
	public void remove(HashMap<BigInteger, String> movedData) {
		if(!movedData.isEmpty())
		{
		Iterator<Entry<BigInteger, String>> it = movedData.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<BigInteger, String> pairs = (Map.Entry<BigInteger, String>)it.next();
			dataStore.remove(pairs.getKey(), pairs.getValue());
		}
		}
	}


}
