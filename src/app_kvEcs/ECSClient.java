package app_kvEcs;


public class ECSClient {
	static ECS ecs = new ECS();
	public static void main(String args[]) {
		System.setProperty("file.encoding", "US-ASCII");
		ecs.runScript();
	}
}
