package common;

public class ServerData {
	
	private final String name;
	private final String address;
	private final int port;
	
	/**
	 * Create new Server data
	 * @param name The name of the server, used for logging and user I/O
	 * @param address The IP address of the server
	 * @param port The remote port the server is running on
	 */
	public ServerData(String name, String address, int port) {
		this.address = address;
		this.port = port;
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public String getName() {
		return name;
	}
	@Override
	public boolean equals(Object otherObj)
	{
		ServerData other = (ServerData)otherObj;
		if(this.address.equals(other.address) && this.port == other.port)
			return true;
		else
			return false;
	}
	
}
