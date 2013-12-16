package app_kvEcs;

public class ECSClient {
	static ECS ecs;
	public static void main(String args[]) {
		System.setProperty("file.encoding", "US-ASCII");

		try {
			ecs = new ECS("./ecs.config");
			ecs.initService(8);
		} catch (Exception e) {
			ecs.logger.error("");
		}

	}
}
