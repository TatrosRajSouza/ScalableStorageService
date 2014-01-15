package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedMap;

import logger.LogSetup;

import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Level;
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
	private Random generator;

	protected Logger logger;

	/**
	 * Initialize a new ECS.
	 */
	public ECS() {
		serverRepository = new InfrastructureMetadata();
		storageService = new InfrastructureMetadata();
		hashing = new ConsistentHashing();
		generator = new Random();

		LogSetup ls = new LogSetup("logs\\ecs.log", "ECS", Level.ALL);
		this.logger = ls.getLogger();
	}

	/**
	 * Initialize and start numberOfNodes server nodes.
	 * @param numberOfNodes Number of server nodes that are going to be initialized.
	 */
	public void initService(int numberOfNodes) {
		for (int i = 0; i < numberOfNodes; i++) {
			ECSServerCommunicator node = initNode();
			if (node == null) {
				logger.warn("Trying to initialize more nodes that are available");
			}
		}
		
		try {
			logger.info("Initializing the metadata for the servers.");
			ECSServerCommunicator server;
			ECSMessage message = new ECSMessage(ECSStatusType.INIT, storageService);
			for (ServerData kvServer : storageService.getServers()) {
				server = (ECSServerCommunicator) kvServer;
				server.sendMessage(message.toBytes());
			}
		} catch (InvalidMessageException e) {
			logger.error("Problems creating the message. Please check the protocol specification."	);
		} catch (SocketTimeoutException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		} catch (IOException e) {
			logger.error("Couldn't send the message. Check with the server is on and try again.");
		}
	}

	/**
	 * Add a new node to the storage service at an arbitrary position.
	 */
	public void addNode() {
		ECSMessage message;
		ECSServerCommunicator node = initNode();
		if (node == null) {
			return;
		}
		ECSServerCommunicator nextNode = getNextNode(node);
		if (nextNode == null) {
			return;
		}
		ECSServerCommunicator auxNode;


		try {
			logger.info("Initializing the metadata and starting the added server.");
			message = new ECSMessage(ECSStatusType.INIT, storageService);
			node.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.START);
			node.sendMessage(message.toBytes());
			
			logger.info("Moving the metadata to the added node.");
			message = new ECSMessage(ECSStatusType.LOCK_WRITE);
			nextNode.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.UPDATE, storageService);
			nextNode.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.MOVE_DATA, getStartIndex(node),
					getEndIndex(node), node);
			nextNode.sendMessage(message.toBytes());
			message = nextNode.receiveMessage();
			if (message.getCommand() != ECSStatusType.MOVE_COMPLETED) {
				logger.error("Unexpected message received. Received  a " + message.getCommand() +
						". Expected: " + ECSStatusType.MOVE_COMPLETED + ". While adding node " +
						node.getAddress() + ":" + node.getPort());
				return;
			}

			logger.info("Updating the metadata in the server nodes.");
			message = new ECSMessage(ECSStatusType.UPDATE, serverRepository);
			for (ServerData server : serverRepository.getServers()) {
				auxNode = (ECSServerCommunicator) server;
				if (auxNode.getPort() != node.getPort() && !auxNode.getAddress().equals(node.getAddress())) {
					auxNode.sendMessage(message.toBytes());
				}
			}

			message = new ECSMessage(ECSStatusType.UNLOCK_WRITE);
			nextNode.sendMessage(message.toBytes());
		} catch (InvalidMessageException e) {
			logger.error("Problems creating the message. Please check the protocol specification.");
		} catch (SocketTimeoutException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		} catch (IOException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		}
	}

	/**
	 * Remove a node from the storage service at an arbitrary position.
	 */
	public void removeNode() {
		ECSMessage message;
		ECSServerCommunicator node = moveRandomNode(storageService, serverRepository);
		if (node == null) {
			logger.warn("Warn. There is no more server nodes available to be removed.");
			return;
		}
		ECSServerCommunicator nextNode = getNextNode(node);
		if (nextNode == null) {
			logger.error("Bug. Error getting nextNode in the removeNode.");
			return;
		}
		BigInteger startIndex = getStartIndex(node);
		BigInteger endIndex = getEndIndex(node);

		logger.info("Removing server " + node.getAddress() + ":" + node.getPort());
		storageService.removeServer(node.getAddress(), node.getPort());
		hashing.removeServer(node.getAddress(), node.getPort());
		try {
			logger.info("Moving data to the next node " + nextNode.getAddress() + ":" + nextNode.getPort());
			message = new ECSMessage(ECSStatusType.LOCK_WRITE);
			node.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.UPDATE, storageService);
			nextNode.sendMessage(message.toBytes());
			message = new ECSMessage(ECSStatusType.MOVE_DATA, startIndex, endIndex, nextNode);
			node.sendMessage(message.toBytes());

			message = node.receiveMessage();
			if (message.getCommand() != ECSStatusType.MOVE_COMPLETED) {
				logger.error("Unexpected message received. Received  a " + message.getCommand() +
						". Expected: " + ECSStatusType.MOVE_COMPLETED + ". While removing node " +
						node.getAddress() + ":" + node.getPort());
				return;
			}

			logger.info("Updating the metadata in the server nodes.");
			message = new ECSMessage(ECSStatusType.UPDATE, storageService);
			for (ServerData server : storageService.getServers()) {
				nextNode = (ECSServerCommunicator) server;
				nextNode.sendMessage(message.toBytes());
			}

			logger.info("Shuting down the node.");
			message = new ECSMessage(ECSStatusType.SHUTDOWN);
			node.sendMessage(message.toBytes());
			node.disconnect();
		} catch (InvalidMessageException e) {
			logger.error("Problems creating the message. Please check the protocol specification.");
		} catch (SocketTimeoutException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		} catch (IOException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		}
	}

	/**
	 * Starts the storage service by calling start() on all KVServer instances that participate in the service.
	 */
	public void start() {
		logger.info("Starting the service.");
		for (ServerData node : storageService.getServers()) {
			startNode((ECSServerCommunicator) node);
		}
	}

	/**
	 * Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
	 */
	public void stop() {
		logger.info("Stopping the service.");
		for (ServerData node : storageService.getServers()) {
			stopNode((ECSServerCommunicator) node);
		}
	}

	/**
	 * Stops all server instances and exits the remote processes.
	 */
	public void shutDown() {
		ECSServerCommunicator serverCommunication;
		ECSMessage ecsMessage;

		logger.info("Shutting down the service.");

		for (ServerData server : storageService.getServers()) {
			serverCommunication = (ECSServerCommunicator) server;

			try {
				ecsMessage = new ECSMessage(ECSStatusType.SHUTDOWN);
				serverCommunication.sendMessage(ecsMessage.toBytes());
				serverCommunication.disconnect();
			} catch (InvalidMessageException e) {
				logger.error("Problems creating the message. Please check the protocol specification.");
			} catch (SocketTimeoutException e) {
				logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
			} catch (IOException e) {
				logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
			}
		}

		serverRepository = new InfrastructureMetadata();
		storageService = new InfrastructureMetadata();
		hashing = new ConsistentHashing();
	}

	/**
	 * Define the server repository reading the fileName configuration file.
	 * @param fileName The name of the configuration file.
	 * @throws NumberFormatException Thrown when the configuration file does not follow the specification.
	 * @throws IOException Thrown when there is a problem reading the configuration file.
	 * @throws IllegalArgumentException Thrown when the configuration file does not follow the specification.
	 */
	public void defineServerRepository(String fileName) throws NumberFormatException, IOException, IllegalArgumentException  {
		BufferedReader br = null;
		String line;

		logger.info("Reading the configurating file and creating the server repository.");
		br = new BufferedReader(new FileReader(fileName));
		while ((line = br.readLine()) != null) {
			String args[] = line.split(" ");
			if (args.length != 3) {
				br.close();
				throw new IllegalArgumentException();
			}
			ECSServerCommunicator server = new ECSServerCommunicator(args[0], args[1], Integer.parseInt(args[2]));
			serverRepository.addServer(server);
		}

		br.close();
	}

	/**
	 * Get the server repository. The repository with non initialized servers.
	 * @return The server repository. The repository with non initialized servers.
	 */
	public InfrastructureMetadata getServerRepository() {
		return serverRepository;
	}

	/**
	 * Get the storage service. The repository with initialized servers.
	 * @return The storage service. The repository with on initialized servers.
	 */
	public InfrastructureMetadata getStorageService() {
		return storageService;
	}

	/**
	 * Get the server hashing circle.
	 * @return The hashing circle.
	 */
	public ConsistentHashing getHashing() {
		return hashing;
	}

	private ECSServerCommunicator initNode() {
		ECSServerCommunicator node = moveRandomNode(serverRepository, storageService);

		if (node == null) {
			logger.warn("Warn. There is no more server nodes available to be initialized.");
			return null;
		}
		logger.info("Initializing server node " + node.getAddress() + ":" + node.getPort());

		sendSSHCall(node.getAddress(), node.getPort());
		hashing.addServer(node.getAddress(), node.getPort());
		
		try {
			node.connect();
		} catch (UnknownHostException e) {
			logger.error("Couldn't reach the server node " + node.getAddress() + ":" + node.getPort());
		} catch (IOException e) {
			logger.error("Couldn't connect to the server node " + node.getAddress() + ":" + node.getPort());
		}

		return node;
	}

	private void startNode(ECSServerCommunicator node) {
		ECSMessage message;

		logger.info("Starting node " + node.getAddress() + ":" + node.getPort());
		try {
			message = new ECSMessage(ECSStatusType.START);
			node.sendMessage(message.toBytes());
		} catch (InvalidMessageException e) {
			logger.error("Problems creating the message. Please check the protocol specification.");
		} catch (SocketTimeoutException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		} catch (IOException e) {
			logger.error("Couldn't communicate with the server node " + node.getAddress() + ":" + node.getPort());
		}
	}

	private void stopNode(ECSServerCommunicator node) {
		ECSMessage message;

		logger.info("Stopping node " + node.getAddress() + ":" + node.getPort());
		try {
			message = new ECSMessage(ECSStatusType.STOP);
			node.sendMessage(message.toBytes());
		} catch (InvalidMessageException e) {
			logger.error("Problems creating the message. Please check the protocol specification.");
		} catch (SocketTimeoutException e) {
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		} catch (IOException e) {
			logger.error("Couldn't communicate with the server node " + node.getAddress() + ":" + node.getPort());
		}
	}

	private ECSServerCommunicator moveRandomNode(InfrastructureMetadata from, InfrastructureMetadata to) {
		ECSServerCommunicator node;		

		node = getRandomNode(from);
		if (node == null) {
			return null;
		}
		to.addServer(node);
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
		Iterator<BigInteger> iterator = hashCircle.keySet().iterator();
		BigInteger startIndex = null;
		BigInteger endIndex = null;

		if (iterator.hasNext()) {
			startIndex = hashCircle.lastKey();
		} else {
			return null;
		}

		while (iterator.hasNext()) {
			endIndex = iterator.next();
			String[] server = hashCircle.get(endIndex).split(":");
			String address = server[0];
			int port = Integer.parseInt(server[1]);
			if (node.getPort() == port && node.getAddress().equals(address)) {
				break;
			}
			startIndex = endIndex;
		}

		return startIndex;
	}

	private BigInteger getEndIndex(ECSServerCommunicator node) {
		SortedMap<BigInteger, String> hashCircle = hashing.getHashCircle();
		for (BigInteger endIndex : hashCircle.keySet()) {
			String[] server = hashCircle.get(endIndex).split(":");
			String address = server[0];
			int port = Integer.parseInt(server[1]);
			if (node.getPort() == port && node.getAddress().equals(address)) {
				return endIndex;
			}
		}
		return null;
	}

	private ECSServerCommunicator getNextNode(ECSServerCommunicator node) {
		SortedMap<BigInteger, String> hashCircle = hashing.getHashCircle();
		boolean next = false;

		for (BigInteger hashValue : hashCircle.keySet()) {
			if (next) {
				return getServer(hashValue);
			} else if (hashCircle.get(hashValue).equals(node.getAddress() + ":" + node.getPort())) {
				next = true;
			}
		}

		if (next) {
			return getServer(hashCircle.firstKey());
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
		Runtime run = Runtime.getRuntime();
		try {
			// This it the original Linux/Unix command
			if (SystemUtils.IS_OS_LINUX) { // Use SSH Command for Linux
				String currentDirectory = System.getProperty("user.dir");
				String[] cmd = {"./script.sh", address, currentDirectory, Integer.toString(port)};
				run.exec(cmd);
			} else { // Otherwise Assume Windows
				new ServerRunner(port).start();   
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
			logger.error("Couldn't send the message within the established time. Check with the server is on and try again.");
		}
	}
}
