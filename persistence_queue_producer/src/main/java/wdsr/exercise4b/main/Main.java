package wdsr.exercise4b.main;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import wdsr.exercise4b.producer.Producer;

public class Main {

	private static final Logger log = LogManager.getLogger(Main.class);

	private static final int TRANCHE_NUMBER = 10000;

	public static void main(String[] args) {
		try {
			log.info(String.format("%d persistent messages sent in {%d} milliseconds", TRANCHE_NUMBER, persistentMessagesActions()));
			log.info(String.format("%d non persistent messages sent in {%d} milliseconds", TRANCHE_NUMBER, nonPersistentMessagesActions()));
		} catch (JMSException e) {
			log.error("Caught: " + e);
			e.printStackTrace();
		}
	}

	public static long persistentMessagesActions() throws JMSException {
		Producer producer = new Producer();
		producer.startUp();

		long start = System.nanoTime();
		for (int sequenceNumber = 0; sequenceNumber < TRANCHE_NUMBER; sequenceNumber++) {
			producer.produceMessage(DeliveryMode.PERSISTENT, sequenceNumber);
		}
		long stop = System.nanoTime();
		producer.close();
		return (stop - start);
	}
	
	public static long nonPersistentMessagesActions() throws JMSException {
		Producer producer = new Producer();
		producer.startUp();

		long start = System.nanoTime();
		for (int sequenceNumber = TRANCHE_NUMBER; sequenceNumber < (2*TRANCHE_NUMBER); sequenceNumber++) {
			producer.produceMessage(DeliveryMode.NON_PERSISTENT, sequenceNumber);
		}
		long stop = System.nanoTime();
		producer.close();
		return (stop - start);
	}
}
