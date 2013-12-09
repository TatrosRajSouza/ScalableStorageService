package app_kvEcs;

import java.math.BigInteger;

//TODO change this for the code that someone will create. Maybe not..
public class KVServerNode/* implements Comparable<KVServerNode>*/{
	private String name;
	private String address;
	private int port;
	private BigInteger startIndex;
//	private String endIndex;
	private KVServerNode nextNode;
	
	public KVServerNode(String name, String address, int port) {
		this.name = name;
		this.address = address;
		this.port = port;
		startIndex = ECS.hashing.hashServer(address, port);
	}
	
	/*@Override
	public int compareTo(KVServerNode compared) {
		String start = new String(startIndex);
		return start.compareTo(new String(compared.getStartIndex()));
	}*/
	
	/**
	 * Verifies if node is inserted in the hash ring it will enter in the
	 * position between this and this.getNextNode()
	 * @param node
	 * @return
	 */
	public boolean isNext(KVServerNode node) {
		if (this.equals(this.getNextNode())) {
			return true;
		}
		if (this.startIndex.compareTo(node.getStartIndex()) < 0
				&& node.getStartIndex().compareTo(nextNode.getStartIndex()) < 0) {
			return true;
		}
		if (nextNode.getStartIndex().compareTo(this.startIndex) < 0
				&& this.startIndex.compareTo(node.getStartIndex()) < 0) {
			return true;
		}
		if (node.getStartIndex().compareTo(nextNode.getStartIndex()) < 0
				&& nextNode.getStartIndex().compareTo(this.startIndex) < 0) {
			return true;
		}
		return false;
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

	public BigInteger getStartIndex() {
		return startIndex;
	}

/*	public void setEndIndex(String address, int port) {
		StringBuffer sb = new StringBuffer();
		byte[] end = ECS.hashing.hashServer(address, port);
		BigInteger endValue = new BigInteger(end).subtract(new BigInteger("1"));
		end = endValue.toByteArray();
		
		for (int i = 0; i < end.length; i++) {
			sb.append(Integer.toString((end[i] & 0xff) + 0x100, 16).substring(1));
		}
		endIndex = sb.toString();
	}
*/
	public KVServerNode getNextNode() {
		return nextNode;
	}

	public void setNextNode(KVServerNode nextNode) {
		this.nextNode = nextNode;
	}

	/*@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof KVServerNode) {
			KVServerNode node = (KVServerNode) obj;
			if (this.name.equals(node.getName())
					&& this.address.equals(node.getAddress())
					&& this.port == node.getPort()) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        result = prime * result + address.hashCode();
        result = prime * result + port;
        return result;
    }*/
}
