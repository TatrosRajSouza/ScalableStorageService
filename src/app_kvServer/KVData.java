package app_kvServer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class KVData {
	public ConcurrentHashMap<Integer, String> dataStore = new ConcurrentHashMap<Integer, String>();
	public KVData()
	{

	}

	public String put(int key, String value) {
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
	public void moveData(HashMap<Integer,String> movingData)
	{
		if(dataStore.size() > 0)
		{
			Iterator<Map.Entry<Integer,String>> it = dataStore.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Integer,String> pairs = (Map.Entry<Integer,String>)it.next();
				dataStore.put(pairs.getKey(), pairs.getValue());
			}
		}
		else
			dataStore.putAll(movingData);

	}

	public String get(int key)  {
		// TODO Auto-generated method stub
		return dataStore.get(key);
	}

	public HashMap<Integer,String> findMovingData(int startIndex, int endIndex)
	{
		int key;
		HashMap<Integer,String> movingData = new HashMap<Integer,String>();
		// iterate over the range or hashmap?
		Iterator<Map.Entry<Integer,String>> it = dataStore.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer,String> pairs = (Map.Entry<Integer,String>)it.next();
			key = pairs.getKey();
			if(startIndex < key && key < endIndex)
			{
				movingData.put(key, pairs.getValue());
			}
		}
		return movingData;
	}

	public void remove(HashMap<Integer, String> movedData) {
		// TODO Auto-generated method stub
		
	}


}
