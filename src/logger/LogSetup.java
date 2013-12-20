package logger;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Represents the initialization for the server logging with Log4J.
 */
public class LogSetup {

	public static final String UNKNOWN_LEVEL = "UnknownLevel";
	// private static Logger logger = Logger.getRootLogger();
	private Logger logger = Logger.getRootLogger();
	private String logdir;
	
	/**
	 * Initializes the logging for the echo server. Logs are appended to the 
	 * console output and written into a separated server log file at a given 
	 * destination.
	 * 
	 * @param logdir the destination (i.e. directory + filename) for the 
	 * 		persistent logging information.
	 * @throws IOException if the log destination could not be found.
	 */
	public LogSetup(String logdir, String name, Level level) {
		this.logdir = logdir;
		initLog(name, logdir, level);
	}
	
	public LogSetup(String logdir, String name, Level level, boolean simple) {
		this.logdir = logdir;
		initLogSimple(name, logdir, level);
	}	
	
	public void initLog(String name, String dir, Level level) {
		// create logger
		logger = Logger.getLogger(name);

		FileAppender fileAppender;
		try {
			PatternLayout layout = new PatternLayout( "%d{ISO8601} %-5p [%t] %c: %m%n" );
			fileAppender = new FileAppender(layout, dir);
			
			logger.removeAllAppenders();
			logger.addAppender(fileAppender);
			// logger.addAppender(new ConsoleAppender(layout));
			logger.setLevel(level);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void initLogSimple(String name, String dir, Level level) {
		// create logger
		logger = Logger.getLogger(name);

		// create log file, where messages will be sent, 
		// you can also use console appender
		FileAppender fileAppender;
		try {
			fileAppender = new FileAppender(new PatternLayout(), dir);
			
			logger.removeAllAppenders();
			logger.addAppender(fileAppender);
			logger.setLevel(level);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Logger getLogger() {
		return this.logger;
	}
	
	public static boolean isValidLevel(String levelString) {
		boolean valid = false;
		
		if(levelString.equals(Level.ALL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.DEBUG.toString())) {
			valid = true;
		} else if(levelString.equals(Level.INFO.toString())) {
			valid = true;
		} else if(levelString.equals(Level.WARN.toString())) {
			valid = true;
		} else if(levelString.equals(Level.ERROR.toString())) {
			valid = true;
		} else if(levelString.equals(Level.FATAL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.OFF.toString())) {
			valid = true;
		}
		
		return valid;
	}
	
	public static String getPossibleLogLevels() {
		return "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF";
	}
}
