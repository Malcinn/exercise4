package wdsr.exercise4c.publisher;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Publisher {
	
	private static final Logger log = LogManager.getLogger(Publisher.class);

	private final String TOPIC_NAME = "MALCINN.TOPIC";

	private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

	private Connection connection = null;

	private Session session = null;

	private Destination destination = null;

	MessageProducer messageProducer = null;

	public Publisher() throws JMSException {
		// create connection
		this.connection = this.connectionFactory.createConnection();
		// create session
		this.session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
		// create destination - topic
		this.destination = session.createTopic(TOPIC_NAME);
		// create Message producer form session to destination - topic
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

	public void publishMessage(int deliveryMode, int sequenceNumber) {
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
