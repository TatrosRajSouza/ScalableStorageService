package app_kvEcs;

public class ECSClient {
	static ECS ecs = new ECS();
	public static void main(String args[]) {
		System.setProperty("file.encoding", "US-ASCII");
		try {
			ecs.defineServerRepository("./ecs.config");
			ecs.initService(8);
			/*ecs.runScript("127.0.0.1", 50000);
			Thread.sleep(500);
			new KVCommunication("127.0.0.1", 50000);*/
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
