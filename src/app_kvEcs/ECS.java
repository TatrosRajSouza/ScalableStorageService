package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

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

	public void addNode() {
		/*ECSServerCommunicator  node = */initNode();

		//Initialize the new storage server with the updated meta­data and start it.
		//Set write lock (lockWrite()) on the successor node;
		//Invoke the transfer of the affected data items to the new storage server:
		//	successor.moveData(range, newServer)
		//When all affected data has been transferred:
		//	Send a meta­data update to all storage servers
		//	Release the write lock on the successor node and finally remove the data items
		//	that are no longer handled by this server
	}

	public void removeNode() {
		/*ServerData node = */moveRandomNode(storageService, serverRepository);
		hashing.update(storageService.getServers());
		//TODO add a removeServer() na InfrastructureMetadata e substituir esse update
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
			moveRandomNode(storageService, serverRepository);
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

	private void sendSSHCall(String address, int port) {
		//Process proc;
		String[] args = {"./script.sh", address, Integer.toString(port)};

		Runtime run = Runtime.getRuntime();
		try {
			//proc = run.exec(script);
			//run.exec("./script.sh");
			run.exec(args);
			//TODO figure out where the output is going
		} catch (IOException e) {
			//TODO deal with this exception
			e.printStackTrace();
		}
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
		int randomIndex;
		ArrayList<ServerData> servers = from.getServers();

		if (servers.isEmpty()) {
			return null;
		}

		randomIndex = generator.nextInt(servers.size());
		node = (ECSServerCommunicator) servers.get(randomIndex);

		to.addServer(node.getName(), node.getAddress(), node.getPort());
		from.removeServer(node.getAddress(), node.getPort());
		return node;
	}
}