package app_kvEcs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ECS {

	private Set<KVStorageServer> serverRepository = new HashSet<KVStorageServer>();
	
	/**
	 * 
	 * @param fileName
	 * @throws IOException
	 */
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
			serverRepository.add(new KVStorageServer(args[0], args[1], args[2]));
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

	public Set<KVStorageServer> getServerRepository() {
		return serverRepository;
	}
}
