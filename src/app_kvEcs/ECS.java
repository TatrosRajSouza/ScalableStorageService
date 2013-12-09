package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import common.messages.InfrastructureMetadata;
import common.messages.ServerData;
import consistent_hashing.ConsistentHashing;

public class ECS {

	private InfrastructureMetadata serverRepository;
	private InfrastructureMetadata storageService;
	private ConsistentHashing hashing;
	private boolean running;
	private Random generator;

	public ECS() {
		serverRepository = new InfrastructureMetadata();
		storageService = new InfrastructureMetadata();
		hashing = new ConsistentHashing();
		running = false;
		generator = new Random();
	}

	public void initService(int numberOfNodes) {
		for (int i = 0; i < numberOfNodes; i++) {
			addNode();
		}
	}

	public void addNode() {
		String address;
		int port;
		
		ServerData node = moveRandomNode(serverRepository, storageService);
		if (node == null) {
			//TODO exception or something like that
			return;
		} else {
			address = node.getAddress();
			port = node.getPort();
		}
		hashing.addServer(address, port);
		runScript(address, port);
		//TODO establish connection?
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
			startNode(node);
		}
		running = true;
	}

	private void startNode(ServerData node) {
		// TODO Auto-generated method stub
		
	}
	
	public void stop() {
		for (ServerData node : storageService.getServers()) {
			stopNode(node);
		}
		running = false;
	}

	private void stopNode(ServerData node) {
		// TODO Auto-generated method stub
		
	}

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

	private void runScript(String address, int port) {
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

	public InfrastructureMetadata getServerRepository() {
		return serverRepository;
	}

	public InfrastructureMetadata getStorageService() {
		return storageService;
	}
	
	public ConsistentHashing getHashing() {
		return hashing;
	}

	public boolean isRunning() {
		return running;
	}

	private ServerData moveRandomNode(InfrastructureMetadata from, InfrastructureMetadata to) {
		ServerData node;
		int randomIndex;
		ArrayList<ServerData> servers = from.getServers();

		if (servers.isEmpty()) {
			return null;
		}

		randomIndex = generator.nextInt(servers.size());
		node = servers.get(randomIndex);
		
		to.addServer(node.getName(), node.getAddress(), node.getPort());
		from.removeServer(node.getAddress(), node.getPort());
		return node;
	}

}