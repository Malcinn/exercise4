package wdsr.exercise4b.producer;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Producer {

	private static final Logger log = LogManager.getLogger(Producer.class);
	
	private final String QUEUE_NAME = "MALCINN.QUEUE";

	private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

	private Connection connection = null;

	private Session session = null;

	private Destination destination = null;

	MessageProducer messageProducer = null;

	public Producer() throws JMSException {
		// create connection
		this.connection = this.connectionFactory.createConnection();
		// create session
		this.session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
		// create destination - queue
		this.destination = session.createQueue(QUEUE_NAME);
		// create Message producer form session to destination - queue
		this.messageProducer = this.session.createProducer(this.destination);
	}

	public void startUp() throws JMSException {
		if (this.connection != null && this.session != null)
			this.connection.start();
	}

	public void close() throws JMSException {
		if (this.connection != null && this.session != null) {
			this.messageProducer.close();
			this.session.close();
			this.connection.close();
		}
	}

	public void produceMessage(int deliveryMode, int sequenceNumber) {
		try {
			// set up delivery mode
			messageProducer.setDeliveryMode(deliveryMode);
			// Create message
			Message textMessage = this.session.createTextMessage("test_" + sequenceNumber);
			// Invoke MessageProducer send method
			messageProducer.send(textMessage);
		} catch (JMSException e) {
			log.error("Caught: " + e);
			e.printStackTrace();
		}
	}

}
