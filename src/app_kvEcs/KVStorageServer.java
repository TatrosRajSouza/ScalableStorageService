package app_kvEcs;

//TODO change this for the code that someone will create
public class KVStorageServer {
	private String name;
	private String address;
	private int port;
	
	public KVStorageServer(String name, String address, String port) {
		this.name = name;
		this.address = address;
		this.port = Integer.parseInt(port);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
