package wdsr.exercise4c.subscriber;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Subscriber {

	private final String TOPIC_NAME = "MALCINN.TOPIC";

	private static final int TIMEOUT = 1000;

	private static final Logger log = LogManager.getLogger(Subscriber.class);

	private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

	private Connection connection = null;

	private Session session = null;

	private Destination destination = null;

	private MessageConsumer messageConsumer = null;

	public Subscriber() throws JMSException {
		// create connection
		this.connection = this.connectionFactory.createConnection();
		// create session
		this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// create destination - queue
		this.destination = this.session.createTopic(TOPIC_NAME);
		// create message consumer from session to destination - queue
		this.messageConsumer = this.session.createConsumer(this.destination);
	}

	public void startUp() throws JMSException {
		if (this.connection != null && this.session != null)
			this.connection.start();
	}

	public void close() throws JMSException {
		if (this.connection != null && this.session != null) {
			this.messageConsumer.close();
			this.session.close();
			this.connection.close();
		}
	}

	public boolean consumeMessage() throws JMSException {
		Message message = null;
		message = this.messageConsumer.receive(TIMEOUT);
		if (message != null) {
			if (message instanceof TextMessage) {
				log.info(((TextMessage) message).getText());
			}
			return true;
		}
		return false;
	}
}
