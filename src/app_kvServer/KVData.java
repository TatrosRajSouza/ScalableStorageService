package app_kvServer;

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


	public String get(int key)  {
		// TODO Auto-generated method stub
		return dataStore.get(key);
	}

	public void moveData(KVServer kvserver) {
		// TODO Auto-generated method stub
		boolean flag = true;
		// iterate over the range or hashmap?
		 Iterator<Map.Entry<Integer,String>> it = dataStore.entrySet().iterator();
		    while (it.hasNext()) {
		    	Map.Entry<Integer,String> pairs = (Map.Entry<Integer,String>)it.next();
		    }
		for (Map.Entry<Integer, String> entry : dataStore.entrySet())
		{
			
		    System.out.println(entry.getKey() + "/" + entry.getValue());
		}
		
	}
}
