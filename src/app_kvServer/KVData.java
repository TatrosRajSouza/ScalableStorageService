package app_kvServer;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class KVData {
	public ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<String, String>();
	public KVData()
	{

	}

	public String put(String key, String value) {
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


	public String get(String key)  {
		// TODO Auto-generated method stub
		return dataStore.get(key);
	}

	public void moveData(KVServer kvserver) {
		// TODO Auto-generated method stub
		boolean flag = true;
		// iterate over the range or hashmap?
		 Iterator<Map.Entry<String,String>> it = dataStore.entrySet().iterator();
		    while (it.hasNext()) {
		    	Map.Entry<String,String> pairs = (Map.Entry<String,String>)it.next();
		    }
		for (Map.Entry<String, String> entry : dataStore.entrySet())
		{
			
		    System.out.println(entry.getKey() + "/" + entry.getValue());
		}
		
	}
}
