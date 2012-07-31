package com.btoddb.flume.channels.hornetq;

import static org.junit.Assert.*;

import java.util.Date;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.junit.Test;

public class HornetQTest {

    @Test
    public void testIt() {
        try {
            // Step 1. Create the Configuration, and set the properties accordingly
            Configuration configuration = new ConfigurationImpl();
            configuration.setPersistenceEnabled(false);
            configuration.setSecurityEnabled(false);
            configuration.getAcceptorConfigurations().add(
                    new TransportConfiguration(InVMAcceptorFactory.class.getName()));

            // Step 2. Create and start the server
            HornetQServer server = HornetQServers.newHornetQServer(configuration);
            server.start();

            // Step 3. As we are not using a JNDI environment we instantiate the objects directly
            ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                    InVMConnectorFactory.class.getName()));
            ClientSessionFactory sf = serverLocator.createSessionFactory();

            // Step 4. Create a core queue
            ClientSession coreSession = sf.createSession(false, false, false);

            final String queueName = "queue.exampleQueue";

            coreSession.createQueue(queueName, queueName, true);

            coreSession.close();

            ClientSession session = null;

            try {
                // send message
                session = sf.createTransactedSession();
                assertTrue(!session.isAutoCommitAcks());
                assertTrue(!session.isAutoCommitSends());
                ClientProducer producer = session.createProducer(queueName);
                ClientMessage message = session.createMessage(false);
                final String propName = "myprop";
                message.putStringProperty(propName, "Hello sent at " + new Date());
                producer.send(message);
                session.commit();
                session.close();
                assertTrue(session.isClosed());

                // receive 1
                session = sf.createTransactedSession();
                ClientConsumer messageConsumer = session.createConsumer(queueName);
                session.start();
                ClientMessage messageReceived = messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                messageReceived.acknowledge();
                session.commit();
                session.close();

                // receive 2
                session = sf.createTransactedSession();
                messageConsumer = session.createConsumer(queueName);
                session.start();
                messageReceived = messageConsumer.receive(1000);
                assertNull(messageReceived);
                session.commit();
                session.close();
            }
            finally {
                if (sf != null) {
                    sf.close();
                }
                server.stop();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
