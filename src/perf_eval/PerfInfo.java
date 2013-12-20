package perf_eval;

public class PerfInfo {
	private double latencyPut = 0.0;
	private double latencyGet = 0.0;
	
	private double throughputBPS = 0.0;
	
	int valueCount = 0;
	
	public void update(double throughput, double latencyPutNew, double latencyGetNew) {
		valueCount++;
		
		throughputBPS = throughputBPS + ((throughput - throughputBPS) / valueCount);
		latencyPut = latencyPut + ((latencyPutNew - latencyPut) / valueCount);
		latencyGet = latencyGet + ((latencyGetNew - latencyGet) / valueCount);
	}
}
