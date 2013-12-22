package app_kvEcs;

import java.io.IOException;

public class ServerRunner implements Runnable {
    String name;
    Thread thread;     
    int port;
    String cmd;
    
    public ServerRunner(int port) {
    	this.port = port;
    	//this.cmd = cmd;
    }       
    public void start () {
        thread = new Thread (this);
        thread.start ();
    }       
    public void run () {
    	
		/* Build the Command String */
		/* We want java -jar <PROGRAM_DIR>\ms3-server.jar port */
		/* Ignoring ssh for now... since you have to enter password and also confirm a few warnings on windows */
		StringBuilder sb = new StringBuilder();
		//sb.append("java -jar ");
		sb.append("java -jar ");
		sb.append(System.getProperty("user.dir") + "\\ms3-server.jar ");
		sb.append(port);
		
		String cmd = sb.toString();
		System.out.println();
		System.out.println("Command: " + cmd);
		System.out.println();
		
		
		Process p;
		try {
			p = Runtime.getRuntime().exec(cmd);
	 
			ReadStream s1 = new ReadStream("stdin", p.getInputStream ());
			ReadStream s2 = new ReadStream("stderr", p.getErrorStream ());
			s1.start ();
			s2.start ();
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				System.out.println("Process interrupted: " + e.getMessage());
				e.printStackTrace();
			}    
		} catch (IOException e1) {
			System.out.println("Unable to create process. IOException: " + e1.getMessage());
			e1.printStackTrace();
		} 
    }
}

