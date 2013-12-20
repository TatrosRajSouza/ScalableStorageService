package perf_eval;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.InvalidMessageException;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import app_kvClient.KVClient;

public class ClientWrapper implements Runnable {
	public static String defaultServer = "127.0.0.1";
	public static int defaultPort = 50000;
	
	Evaluator evalInstance;
	KVClient clientInstance;
	private String name;
	private HashMap<String, String> requestMap = null;
	private ArrayList<String> keys = null;
	private ArrayList<String> getKeys = new ArrayList<String>();
	private int putsSent = 0;
	private int putsSuccess = 0;
	private int putsFailed = 0;
	
	private int getsSent = 0;
	private int getsSuccess = 0;
	private int getsFailed = 0;
	
	public Logger logger;
	public Logger perfLog;
	
	public ClientWrapper(String name, Evaluator instance) {
		this.evalInstance = instance;
		this.clientInstance = new KVClient(name);
		this.name = name;
		
		LogSetup ls = new LogSetup("logs\\wrapper.log", "Wrapper " + name, Level.ALL);
		this.logger = ls.getLogger();
		
		LogSetup ls2 = new LogSetup("logs\\perf.log", "PerfLog " + name, Level.ALL);
		this.perfLog = ls2.getLogger();
	}
	
	public void run() {
		if (requestMap == null) {
			evalInstance.getLogger().warn(this.getName() + " has an empty request map. Exiting.");
			System.exit(1);
		}

		try {
			clientInstance.connect(defaultServer, defaultPort);
			
			long startTime = 0;
			long elapsedTime = 0;
			long bitsSecond = 0;
			double timerStart = (double) System.nanoTime() /  1000000;
			double timerNextPing = timerStart + 1000; 
			while (keys.size() > 0) {
				// report bps each second
				if (((double)System.nanoTime() / 1000000) >= timerNextPing) {
					evalInstance.getPerfInfo(this).updateThroughput(bitsSecond);
					bitsSecond = 0;
				}
				
				/* Put a random key value pair from the clients kv map */
				Random rand = new Random();
				int value = rand.nextInt(keys.size());
				

				
				startTime = System.nanoTime(); 				
				KVMessage result = clientInstance.put(keys.get(value), requestMap.get(keys.get(value)));
				elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
				double elapsedTimePut = (double)elapsedTime / 1000000; // to ms
				
				requestMap.remove(keys.get(value));
				getKeys.add(keys.remove(value));
				
				putsSent++;
				if (result != null && (result.getStatus().equals(StatusType.PUT_SUCCESS) || result.getStatus().equals(StatusType.PUT_UPDATE))) {
					logger.info("Successfully put <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
					perfLog.info("Put latency: " + elapsedTimePut);
					putsSuccess++;
					
					String dataSent = result.getValue() + result.getKey();
					bitsSecond += dataSent.getBytes().length * 8;
					
					perfLog.info("PUT BPS added: " + dataSent.getBytes().length * 8 + "for: " + dataSent);
					
					evalInstance.updateData(result.getKey());
				} else {
					putsFailed++;
					logger.info("Put Failed <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
					perfLog.info("Put latency: " + elapsedTimePut);
				}
				
				//evalInstance.getLogger().info(this.name + " processing. Puts sent: " + putsSent + ", Puts Success: " + putsSuccess + ", Puts failed: " + putsFailed);
				//evalInstance.getLogger().info("                       Gets sent: " + getsSent + ", Gets Success: " + getsSuccess + ", Gets failed: " + getsFailed);
				

				
				/* Get a random key from available Data */
				/*
				int numKeys = this.getKeys.size();
				if (numKeys > 0) {
					Random rand2 = new Random();
					int value2 = rand2.nextInt(numKeys);
					
					String randKey = getKeys.get(value2);
					KVMessage result2 = clientInstance.get(randKey);
					
					if (result2 != null && (result2.getStatus().equals(StatusType.GET_SUCCESS))) {
						logger.info("Successfully got <" + result2.getKey() + ", " + result2.getValue() + "> (" + result2.getStatus().toString() + ")");
						perfLog.info("Get latency: ");
						getsSuccess++;
					} else {
						getsFailed++;
						logger.info("Get Failed <" + result2.getKey() + ", " + result2.getValue() + "> (" + result2.getStatus().toString() + ")");
						perfLog.info("Get latency: ");
					}
				}*/
				
				int numAvailableKeys = evalInstance.getNumAvailableKeys();
				if (numAvailableKeys > 0) {
					value = rand.nextInt(evalInstance.getNumAvailableKeys());
					String randomKey = evalInstance.getAvailableKey(value);
					
					if (randomKey == null) {
						evalInstance.getLogger().error(name + " TRIED TO GET INVALID KEY: " + value);
						System.exit(1);
					}
					
					startTime = System.nanoTime();
					// evalInstance.getLogger().info("Trying to get " + randomKey);
					result = clientInstance.get(randomKey);				
					elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
					
					// retry get
					if (result.getStatus().equals(StatusType.GET_ERROR)) {
						startTime = System.nanoTime();
						result = clientInstance.get(randomKey);
						elapsedTime = System.nanoTime() - startTime; // elapsed time in nano seconds
					}
					
					double elapsedTimeGet = (double)elapsedTime / 1000000; // to ms
					
					getsSent++;
					if (result != null && (result.getStatus().equals(StatusType.GET_SUCCESS))) {
						logger.info("Successfully got <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
						perfLog.info("Get latency: " + elapsedTimeGet);
						getsSuccess++;
						
						String dataRec = result.getValue() + result.getKey();
						bitsSecond += dataRec.getBytes().length * 8;
						perfLog.info("GET BPS added: " + dataRec.getBytes().length * 8 + "for: " + dataRec);
					} else {
						getsFailed++;
						logger.info("Get Failed <" + result.getKey() + ", " + result.getValue() + "> (" + result.getStatus().toString() + ")");
						perfLog.info("Get latency: " + elapsedTimeGet);
					}
					
					evalInstance.getPerfInfo(this).update(elapsedTimePut, elapsedTimeGet);
				}
			}
			
			double LatencyGetAvg = Math.round(evalInstance.getPerfInfo(this).getLatencyGet());
			double LatencyPutAvg = Math.round(evalInstance.getPerfInfo(this).getLatencyPut());
			double bpsAvg = Math.round(evalInstance.getPerfInfo(this).getThroughpout());
			
			evalInstance.getLogger().info(this.name + " finished. Puts sent: " + putsSent + ", Puts Success: " + putsSuccess + ", Puts failed: " + putsFailed);
			evalInstance.getLogger().info("                       Gets sent: " + getsSent + ", Gets Success: " + getsSuccess + ", Gets failed: " + getsFailed);
			evalInstance.getLogger().info("                       Avg Get Latency: " + LatencyGetAvg + ", Avg Put Latency: " + LatencyPutAvg + ", Avg bps: " + bpsAvg);
		} catch (ConnectException e) {
			evalInstance.getLogger().error("ConnectException: " + e.getMessage());
		} catch (UnknownHostException e) {
			evalInstance.getLogger().error("UnknownHostException: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			evalInstance.getLogger().error("IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InvalidMessageException e) {
			evalInstance.getLogger().error("InvalidMessageException: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public KVClient getClient() {
		return this.clientInstance;
	}
	
	public String getName() {
		return this.name;
	}
	
	public void setRequestMap(HashMap<String, String> map) {
		this.requestMap = map;
		
		if (keys == null)
			keys = new ArrayList<String>();
		
		keys.clear();
		for (String key : requestMap.keySet()) {
			keys.add(key);
		}
	}
}
