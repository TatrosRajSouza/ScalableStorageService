package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import common.messages.ECSMessage;
import common.messages.ECSStatusType;
import common.messages.InfrastructureMetadata;
import common.messages.InvalidMessageException;
import common.messages.ServerData;
import consistent_hashing.ConsistentHashing;

public class ECS {

	private InfrastructureMetadata serverRepository;
	private InfrastructureMetadata storageService;
	private ConsistentHashing hashing;
	//Is this running var really necessary?
	private Random generator;

	protected static Logger logger = Logger.getRootLogger();

	public ECS() {
		serverRepository = new InfrastructureMetadata();
		storageService = new InfrastructureMetadata();
		hashing = new ConsistentHashing();
		generator = new Random();
	}

	public void initService(int numberOfNodes) {
		ECSStatusType command = ECSStatusType.INIT;

		for (int i = 0; i < numberOfNodes; i++) {
			initNode();
		}
		try {
			ECSServerCommunicator server;
			ECSMessage message = new ECSMessage(command, storageService);
			for (ServerData kvServer : storageService.getServers()) {
				server = (ECSServerCommunicator) kvServer;
				server.sendMessage(message.toBytes());
			}
		} catch (InvalidMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void addNode() {
		ECSMessage message;
		ECSServerCommunicator nextNode;
		ECSServerCommunicator node = initNode();

		try {
			message = new ECSMessage(ECSStatusType.INIT, serverRepository);
			node.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.START);
			node.sendMessage(message.toBytes());
			nextNode = getNextNode(node);
			message = new ECSMessage(ECSStatusType.LOCK_WRITE);
			nextNode.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.MOVE_DATA, getStartIndex(nextNode),
					getEndIndex(nextNode), nextNode);
			message = nextNode.receiveMessage();
			if (message.getCommand() != ECSStatusType.MOVE_COMPLETED) {
				//TODO error
			}
			message = new ECSMessage(ECSStatusType.UPDATE, serverRepository);
			for (ServerData server : serverRepository.getServers()) {
				nextNode = (ECSServerCommunicator) server;
				nextNode.sendMessage(message.toBytes());
			}
			message = new ECSMessage(ECSStatusType.UNLOCK_WRITE);
			node.sendMessage(message.toBytes());
		} catch (InvalidMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void removeNode() {
		ECSMessage message;
		ECSServerCommunicator node = getRandomNode(storageService);
		BigInteger startIndex = getStartIndex(node);
		storageService.removeServer(node.getAddress(), node.getPort());
		//TODO add a removeServer() na InfrastructureMetadata e substituir esse update
		hashing.update(storageService.getServers());
		// Recalculate and update the meta­data of the storage service (i.e., the range for the
		// successor node)
		try {
			message = new ECSMessage(ECSStatusType.LOCK_WRITE);
			node.sendMessage(message.toBytes());
			// Set the write lock on the server that has to be deleted.
			//message = new ECSMessage(bytes)
			// Send meta­data update to the successor node (i.e., successor is now also responsible
			// for the range of the server that is to be removed)
			// Invoke the transfer of the affected data items (i.e., all data of the server that is to be
			// removed)  to the successor server. The data that is transferred should not be deleted
			// immediately to be able to serve read requests in the mean time
			// 		serverToRemove.moveData(range, successor)
			// When all affected data has been transferred (i.e., the server that has to be removed
			// sends back a notification to the ECS)
			//		Send a meta­data update to the remaining storage servers.
			//		Shutdown the respective storage server.
		} catch (InvalidMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void start() {
		for (ServerData node : storageService.getServers()) {
			startNode((ECSServerCommunicator) node);
		}
	}

	public void stop() {
		for (ServerData node : storageService.getServers()) {
			stopNode((ECSServerCommunicator) node);
		}
	}

	public void shutDown() {
		ECSServerCommunicator serverCommunication;
		ECSMessage ecsMessage;

		for (ServerData server : storageService.getServers()) {
			serverCommunication = (ECSServerCommunicator) server;

			try {
				ecsMessage = new ECSMessage(ECSStatusType.SHUTDOWN);
				serverCommunication.sendMessage(ecsMessage.toBytes());
				serverCommunication.disconnect();
			} catch (InvalidMessageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SocketTimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		serverRepository = new InfrastructureMetadata();
		storageService = new InfrastructureMetadata();
		hashing = new ConsistentHashing();
	}

	// this is not in the interface specification
	public void defineServerRepository(String fileName) throws Exception {
		BufferedReader br = null;
		String line;

		br = new BufferedReader(new FileReader(fileName));
		while ((line = br.readLine()) != null) {
			String args[] = line.split(" ");
			if (args.length != 3) {
				br.close();
				throw new IllegalArgumentException();
			}
			//TODO exception
			serverRepository.addServer(args[0], args[1], Integer.parseInt(args[2]));
		}

		br.close();
	}

	// this is not in the interface specification
	public InfrastructureMetadata getServerRepository() {
		return serverRepository;
	}

	// this is not in the interface specification
	public InfrastructureMetadata getStorageService() {
		return storageService;
	}

	// this is not in the interface specification
	public ConsistentHashing getHashing() {
		return hashing;
	}

	private ECSServerCommunicator initNode() {
		ECSServerCommunicator node = moveRandomNode(serverRepository, storageService);
		if (node == null) {
			//TODO exception or something like that
			return null;
		}

		sendSSHCall(node.getAddress(), node.getPort());
		hashing.addServer(node.getAddress(), node.getPort());
		try {
			node.connect();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return node;
	}
	
	private void startNode(ECSServerCommunicator node) {
		ECSServerCommunicator serverCommunicator;
		ECSMessage message;
		try {
			message = new ECSMessage(ECSStatusType.START);
			for (ServerData server : storageService.getServers()) {
				serverCommunicator = (ECSServerCommunicator) server;
				serverCommunicator.sendMessage(message.toBytes());
			}
		} catch (InvalidMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void stopNode(ECSServerCommunicator node) {
		ECSServerCommunicator serverCommunicator;
		ECSMessage message;
		try {
			message = new ECSMessage(ECSStatusType.STOP);
			for (ServerData server : storageService.getServers()) {
				serverCommunicator = (ECSServerCommunicator) server;
				serverCommunicator.sendMessage(message.toBytes());
			}
		} catch (InvalidMessageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private ECSServerCommunicator moveRandomNode(InfrastructureMetadata from, InfrastructureMetadata to) {
		ECSServerCommunicator node;		

		node = getRandomNode(from);
		to.addServer(node.getName(), node.getAddress(), node.getPort());
		from.removeServer(node.getAddress(), node.getPort());
		return node;
	}

	private ECSServerCommunicator getRandomNode(InfrastructureMetadata metadata) {
		int randomIndex;
		ArrayList<ServerData> servers = metadata.getServers();

		if (servers.isEmpty()) {
			return null;
		}
		randomIndex = generator.nextInt(servers.size());
		return (ECSServerCommunicator) servers.get(randomIndex);
	}

	private BigInteger getStartIndex(ECSServerCommunicator node) {
		SortedMap<BigInteger, String> hashCircle = hashing.getHashCircle();
		for (BigInteger startIndex : hashCircle.keySet()) {
			String[] server = hashCircle.get(startIndex).split(":");
			String address = server[0];
			int port = Integer.parseInt(server[1]);
			if (node.getPort() == port && node.getAddress().equals(address)) {
				return startIndex;
			}
		}
		return null;
	}
	
	private BigInteger getEndIndex(ECSServerCommunicator node) {
		boolean next = false;
		SortedMap<BigInteger, String> hashCircle = hashing.getHashCircle();
		for (BigInteger startIndex : hashCircle.keySet()) {
			if (next) {
				return startIndex;
			}
			String[] server = hashCircle.get(startIndex).split(":");
			String address = server[0];
			int port = Integer.parseInt(server[1]);
			if (node.getPort() == port && node.getAddress().equals(address)) {
				next = true;
			}
		}
		if (next) {
			return hashCircle.firstKey();
		}
		return null;
	}

	// This method should go in the ConsistentHashing class
	private ECSServerCommunicator getNextNode(ECSServerCommunicator node) {
		SortedMap<BigInteger, String> hashCircle = hashing.getHashCircle();
		boolean next = false;

		for (BigInteger hashValue : hashCircle.keySet()) {
			if (next) {
				getServer(hashValue);
			} else if (hashCircle.get(hashValue).equals(node.getAddress() + ":" + node.getPort())) {
				next = true;
			}
		}

		if (next) {
			getServer(hashCircle.firstKey());
		}

		return null;
	}

	private ECSServerCommunicator getServer(BigInteger hashValue) {
		String[] addressAndPort = hashing.getHashCircle().get(hashValue).split(":");
		String address = addressAndPort[0];
		int port = Integer.parseInt(addressAndPort[1]);
		for (ServerData server : storageService.getServers()) {
			if (server.getPort() == port && server.getAddress().equals(address)) {
				return (ECSServerCommunicator) server;
			}
		}
		return null;
	}

	private void sendSSHCall(String address, int port) {
		String[] args = {"./script.sh", address, Integer.toString(port)};

		Runtime run = Runtime.getRuntime();
		try {
			run.exec(args);
		} catch (IOException e) {
			//TODO deal with this exception
			e.printStackTrace();
		}
	}
}