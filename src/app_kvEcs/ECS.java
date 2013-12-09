package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import consistent_hashing.ConsistentHashing;

public class ECS {

	private Set<KVServerNode> serverRepository;
	private Set<KVServerNode> storageService;
	private Random generator;
	public static ConsistentHashing hashing = new ConsistentHashing();

	public ECS() {
		serverRepository = new HashSet<KVServerNode>();
		storageService = new HashSet<KVServerNode>();
		generator = new Random();
	}

	public void initService(int numberOfNodes) {
		for (int i = 0; i < numberOfNodes; i++) {
			addNode();
		}
	}

	public void addNode() {
		moveRandomNode(serverRepository, storageService);
	}

	public void removeNode() {
		moveRandomNode(storageService, serverRepository);
	}

	private void moveRandomNode(Set<KVServerNode> from, Set<KVServerNode> to) {
		KVServerNode node;
		KVServerNode[] repository;
		int randomIndex;

		if (from.isEmpty()) {
			return;
		}

		repository = new KVServerNode[from.size()];
		from.toArray(repository);
		randomIndex = generator.nextInt(repository.length);
		node = repository[randomIndex];

		if (to.isEmpty()) {
			node.setNextNode(node);
		} else {
			for (KVServerNode kvServerNode : to) {
				if (kvServerNode.isNext(node)) {
					node.setNextNode(kvServerNode.getNextNode());
					kvServerNode.setNextNode(node);
				}
			}
		}

		to.add(node);
		from.remove(node);
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
			serverRepository.add(new KVServerNode(args[0], args[1], Integer.parseInt(args[2])));
		}

		br.close();
	}

	public void runScript() {
		@SuppressWarnings("unused")
		Process proc;
		String script = "./script.sh";

		Runtime run = Runtime.getRuntime();
		try {
			proc = run.exec(script);
			//TODO figure out where the output is going
		} catch (IOException e) {
			//TODO deal with this exception
			e.printStackTrace();
		}
	}

	public Set<KVServerNode> getServerRepository() {
		return serverRepository;
	}

	public Set<KVServerNode> getStorageService() {
		return storageService;
	}

	//TODO Delete this code. Just for testing
	public void printHashRing() {
		for (KVServerNode node : storageService) {
			System.out.print  (node.getStartIndex() + " - ");
			System.out.println(node.getNextNode().getStartIndex());
		}
	}
	public void printHashRing1() {
		for (KVServerNode node : storageService) {
			System.out.print  (node.getStartIndex().toString(16) + " - ");
			System.out.println(node.getNextNode().getStartIndex().subtract(BigInteger.valueOf(1)).toString(16));
		}
	}
}