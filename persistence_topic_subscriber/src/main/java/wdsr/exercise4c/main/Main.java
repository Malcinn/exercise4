package wdsr.exercise4c.main;

import javax.jms.JMSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import wdsr.exercise4c.subscriber.Subscriber;

public class Main {

	private static final Logger log = LogManager.getLogger(Main.class);

	private static int messageCounter = 0;

	public static void main(String[] args) {
		try {
			Subscriber subscriber = new Subscriber();
			subscriber.startUp();

			while (subscriber.consumeMessage()) {
				messageCounter++;
			}
			log.info(String.format("Received %d messages.", messageCounter));

			subscriber.close();
		} catch (JMSException e) {
			log.error("Caught: " + e);
			e.printStackTrace();
		}

	}
}
