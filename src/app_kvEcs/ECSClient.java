package app_kvEcs;

import java.io.IOException;

public class ECSClient {
	static ECS ecs = new ECS();
	public static void main(String args[]) {

		System.setProperty("file.encoding", "US-ASCII");

		if (args.length == 1) {
			String fileName = args[0];
			
			try {
				ecs.defineServerRepository("./" + fileName);
				ECSShell shell = new ECSShell();
				shell.display();
			} catch (NumberFormatException e) {
				System.out.println("Error! Configuration file does not follow the specification.");
			} catch (IllegalArgumentException e) {
				System.out.println("Error! Configuration file does not follow the specification.");
			} catch (IOException e) {
				System.out.println("Error! Couldn't read the configuration file.");
			}
		} else {
			System.out.println("Error! To run the ECS pass the name of the configuration file as an argument."
					+ "The file must be in the same directory as the ECS");
		}

	}
}
