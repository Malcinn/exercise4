package wdsr.exercise4b.main;

import javax.jms.JMSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import wdsr.exercise4b.consumer.Consumer;

public class Main {

	private static final Logger log = LogManager.getLogger(Main.class);
	
	private static int messageCounter = 0;
	
	public static void main(String[] args) {
		try {
			Consumer consumer = new Consumer();
			consumer.startUp();
			
			while (consumer.consumeMessage()){
				messageCounter++;
			}
			log.info(String.format("Received %d messages.", messageCounter));
			
			consumer.close();
		} catch (JMSException e) {
			log.error("Caught: "+e);
			e.printStackTrace();
		}
		
	}

}
