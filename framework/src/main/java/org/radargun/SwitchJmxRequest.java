package org.radargun;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * implements a switch request to infinispan
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SwitchJmxRequest {

   private static final String JMX_DOMAIN = "org.infinispan";
   private static final String DEFAULT_COMPONENT = "ReconfigurableReplicationManager";
   private static final String DEFAULT_JMX_PORT = "9998";
   private static final String PRINT_STATS = "-print-stats";
   private static final int NUM_PARAMS = 2;

   private final ObjectName switchComponent;
   private final String newProtocolId;
   private final MBeanServerConnection mBeanServerConnection;
   private final boolean printOnly;

   public static void main(String[] args) throws Exception {
      if (args.length < NUM_PARAMS) {
         System.err.println("Expected at least " + NUM_PARAMS + " arguments: [" + PRINT_STATS + "] <new protocol> <hostname> [<port> <component>]");
         System.exit(1);
      } else if (PRINT_STATS.equalsIgnoreCase(args[0]) && args.length < NUM_PARAMS + 1) {
         System.err.println("Expected at least " + (NUM_PARAMS + 1) + " arguments: " + PRINT_STATS + " <new protocol> <hostname> [<port> <component>]");
         System.exit(1);
      }

      String newProtocolId;
      String hostname;
      String port = DEFAULT_JMX_PORT;
      String component = DEFAULT_COMPONENT;
      boolean printOnly = false;
      int i = 0;

      if (args[0].equals(PRINT_STATS)) {
         i = 1;
         printOnly = true;
      }

      newProtocolId = args[i++];
      hostname = args[i++];
      if (args.length > i) {
         port = args[i++];
      }
      if (args.length > i) {
         component = args[i];
      }

      new SwitchJmxRequest(component, newProtocolId, hostname, port, printOnly).doRequest();
   }

   public SwitchJmxRequest(String component, String newProtocolId, String hostname, String port, boolean printOnly) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      switchComponent = getCacheComponent(component);
      this.newProtocolId = newProtocolId;
      this.printOnly = printOnly;
   }

   @SuppressWarnings("StringBufferReplaceableByString")
   private ObjectName getCacheComponent(String component) throws Exception {
      for (ObjectName name : mBeanServerConnection.queryNames(null, null)) {
         if (name.getDomain().equals(JMX_DOMAIN)) {

            if ("Cache".equals(name.getKeyProperty("type"))) {
               String cacheName = name.getKeyProperty("name");
               String cacheManagerName = name.getKeyProperty("manager");
               String objectNameString = new StringBuilder(JMX_DOMAIN)
                     .append(":type=Cache,name=")
                     .append(cacheName.startsWith("\"") ? cacheName :
                                   ObjectName.quote(cacheName))
                     .append(",manager=").append(cacheManagerName.startsWith("\"") ? cacheManagerName :
                                                       ObjectName.quote(cacheManagerName))
                     .append(",component=").append(component).toString();
               return new ObjectName(objectNameString);
            }
         }
      }
      return null;
   }

   public void doRequest() throws Exception {
      if (switchComponent == null) {
         throw new NullPointerException("Component does not exists");
      }
      if (printOnly) {
         Object stats = mBeanServerConnection.invoke(switchComponent, "printSwitchAvgDurations", new Object[0], new String[0]);
         System.out.println();
         System.out.println(stats);
         System.out.println();
      } else {
         mBeanServerConnection.invoke(switchComponent, "switchTo", new Object[] {newProtocolId}, new String[] {"java.lang.String"});
         System.out.println("Switch done!");
      }
   }

}
