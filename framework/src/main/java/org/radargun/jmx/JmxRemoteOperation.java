package org.radargun.jmx;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class JmxRemoteOperation {

   private static final String PREFIX = "org.radargun:stage=";
   private static final int NUM_PARAMS = 4;

   public static void main(String[] args) throws Exception {
      if (args.length != NUM_PARAMS) {
         System.err.println("Expected " + NUM_PARAMS + " arguments: <component> <operation> <hostname> <port>");
         System.exit(1);
      }

      executeOperation(args[0], args[1], args[2], args[3]);
   }

   public static void executeOperation(String component, String operation, String hostname, String port) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      System.out.println("Connecting to " + connectionUrl);
      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      MBeanServerConnection mBeanServer = connector.getMBeanServerConnection();
      ObjectName objectName = new ObjectName(PREFIX + component);

      System.out.println("Connected! invoking operation " + operation + " in " + objectName);
      mBeanServer.invoke(objectName, operation, new Object[0], new String[0]);

      System.out.println("Invoked successfully!");
      System.exit(0);
   }

}
