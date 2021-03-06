package common;

import java.util.ArrayList;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class InfrastructureMetadata {
	private ArrayList<ServerData> servers;
	private Logger logger;
	
	/**
	 * Construct new empty meta data
	 */
	public InfrastructureMetadata() {
		LogSetup ls = new LogSetup("logs/metaData.log", "metaData", Level.ALL);
		this.logger = ls.getLogger();
		
		servers = new ArrayList<ServerData>();
	}
	
	/**
	 * Construct new meta data from existing list of serverData
	 * @param servers ArrayList containing a number of ServerData
	 */
	public InfrastructureMetadata(ArrayList<ServerData> servers) {
		LogSetup ls = new LogSetup("logs/metaData.log", "metaData", Level.ALL);
		this.logger = ls.getLogger();
		
		this.servers = servers;
	}
	
	/**
	 * Construct new meta data from existing valid metaDataString obtained from 
	 * toString() of a different InfrastructureMetadata instance.
	 * @param metaDataString valid String representation of meta data, 
	 * i.e.: ServerNameA,ServerIPA,ServerPortA;ServerNameB,ServerIPB,ServerPortB; ... and so on.
	 */
	public InfrastructureMetadata(String metaDataString) {
		LogSetup ls = new LogSetup("logs/metaData.log", "metaData", Level.ALL);
		this.logger = ls.getLogger();
		
		servers = new ArrayList<ServerData>();
		update (metaDataString);
	}
	
	/**
	 * Get the list of active servers according to meta data
	 * @return list of active servers
	 */
	public ArrayList<ServerData> getServers() {
		return servers;
	}
	
	/**
	 * Expand this meta data by adding a new server
	 * @param name Name of the server
	 * @param address IP address of the server
	 * @param port remote port the server is running on
	 */
	public void addServer(String name, String address, int port) {
		servers.add(new ServerData(name, address, port));
	}
	
	/**
	 * Expand this meta data by adding a new server
	 * @param server The server that will be added to this meta data
	 */
	public void addServer(ServerData server) {
		servers.add(server);
	}
	
	/**
	 * Remove the specified server from the meta data
	 * @param address IP address of the server
	 * @param port remote port the server is running on
	 */
	public void removeServer(String address, int port) {
		ArrayList<ServerData> removeList = new ArrayList<ServerData>();
		
		/**
		 * Mark items for removal
		 */
		for (ServerData server : servers) {
			if (server.getAddress() == address) {
				if (server.getPort() == port) {
					removeList.add(server);
				}
			}
		}
		
		/**
		 * Remove marked items
		 */
		for (ServerData server : removeList) {
			servers.remove(server);
		}
	}
	
	/**
	 * Disregard this version of meta data and update it to the data specified in the metadataString 
	 * It is very important that the correct format is used: ServerNameA,ServerIPA,ServerPortA;ServerNameB,ServerIPB,ServerPortB; ... and so on.
	 * @param metadataString A valid String representation of meta data obtained by calling the toString() method
	 */
	public void update(String metadataString) {
		/* Initialize the new server data that will replace the old one */
		ArrayList<ServerData> newServerData = new ArrayList<ServerData>();
		
		if (!metadataString.equals("")) {
			/* Obtain ServerData information from metadataString */
			String[] serversStr = metadataString.split(";"); // Split into "serverStrings"
			
			/* Were we able to obtain at least one server from the provided data? */
			if (serversStr.length <= 0) {
				logger.error("Failed to update meta data to new version.\nThe provided metadataString does not specify at least one Server.\nData: " + metadataString);
				return;
			}
			
			for (String serverStr : serversStr) { // For each serverString...
				String[] serverDataStr = serverStr.split(","); // Split serverString into serverData
				
				if (serverDataStr.length != 3) {
					logger.error("Failed to update meta data to new version.\nThe serverDataString of length " + serverDataStr.length + " did not match the expected length (3).\n" +
							"Data: " + serverStr);
					break;
				}
				
				else {
					String name = serverDataStr[0];
					String address = serverDataStr[1];
					
					try {
						int port = Integer.parseInt(serverDataStr[2]);
						newServerData.add(new ServerData(name, address, port));
					} catch (NumberFormatException ex) {
						logger.error("Failed to update meta data to new version.\nThe serverString contains an invalid port number.\nData: " + serverStr);
						break;
					}
				}
			}
		}
		
		if (newServerData.size() <= 0) {
			logger.warn("The String supplied to the meta-data update includes no active Servers, therefore meta-data is empty.");
		}
		
		/* Update serverData to new version */
		this.servers = newServerData;
	}
	
	/**
	 * Disregard this version of meta data and update it to the data specified in the metadataBytes 
	 * It is very important that the correct format is used, a byte representation of a valid metadataString generated by getBytes() is expected.
	 * @param metadataString A valid Byte representation of meta data obtained by calling the getBytes() method
	 
	public void update(byte[] metadataBytes) {
		this.update(new String(metadataBytes));
	}
	*/
	
	/**
	 * Obtain the String representation for parsing and updating: ServerNameA,ServerIPA,ServerPortA;ServerNameB,ServerIPB,ServerPortB; ... and so on.
	 */
	@Override
	public String toString() {
		StringBuilder stringData = new StringBuilder();
		
		for (ServerData server : servers) {
			stringData.append(server.getName() + "," + server.getAddress() + "," + server.getPort() + ";");
		}
		
		return stringData.toString();
	}
	
	/**
	 * Generate a byte presentation of the "parse string" obtained from toString()
	 * @return byte presentation of this meta data
	 */
	public byte[] getBytes() {
		return this.toString().getBytes();
	}
}
