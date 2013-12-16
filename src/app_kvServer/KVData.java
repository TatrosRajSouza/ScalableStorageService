package app_kvServer;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;



public class KVData {
	public ConcurrentHashMap<BigInteger, String> dataStore = new ConcurrentHashMap<BigInteger, String>();
	public KVData()
	{

	}

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

			returnValue =dataStore.remove(key);

		}
		return returnValue;

	}
	public void moveData(HashMap<BigInteger,String> movingData)
	{
		if(dataStore.size() > 0)
		{
			Iterator<Entry<BigInteger, String>> it = dataStore.entrySet().iterator();
			while (it.hasNext()) {
				Entry<BigInteger, String> pairs = (Map.Entry<BigInteger,String>)it.next();
				dataStore.put(pairs.getKey(), pairs.getValue());
			}
		}
		else
			dataStore.putAll(movingData);

	}

	public String get(BigInteger hashedKey)  {
		// TODO Auto-generated method stub
		return dataStore.get(hashedKey);
	}

	public HashMap<BigInteger,String> findMovingData(BigInteger startIndex, BigInteger endIndex)
	{
		BigInteger key;
		HashMap<BigInteger, String> movingData = new HashMap<BigInteger,String>();
		// iterate over the range or hashmap?
		Iterator<Entry<BigInteger, String>> it = dataStore.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<BigInteger,String> pairs = (Entry<BigInteger, String>)it.next();
			key = pairs.getKey();
			// need modification
			if(startIndex.compareTo(key)  >= 0  && endIndex.compareTo(key) <= 0 )
			{
				movingData.put(key, pairs.getValue());
			}
		}
		return movingData;
	}

	public void remove(HashMap<BigInteger, String> movedData) {
		// TODO Auto-generated method stub
		Iterator<Entry<BigInteger, String>> it = movedData.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<BigInteger, String> pairs = (Map.Entry<BigInteger, String>)it.next();
			dataStore.remove(pairs.getKey(), pairs.getValue());
		}
		
	}


}
