package org.radargun;

import org.radargun.parser.ArgumentsParser;
import org.radargun.parser.Option;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class StopJmxRequest {

   private static final String COMPONENT_PREFIX = "org.radargun:stage=";
   private static final String DEFAULT_COMPONENT = "Benchmark";
   private static final String DEFAULT_JMX_PORT = "9998";
   private final ObjectName benchmarkComponent;
   private final MBeanServerConnection mBeanServerConnection;

   public StopJmxRequest(String component, String hostname, String port) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
   }

   public static void main(String[] args) throws Exception {
      ArgumentsParser parser = new ArgumentsParser(Parameter.asList());
      parser.parse(args);
      System.out.println("Options are " + parser.printOptions());

      if (parser.hasOption(Parameter.HELP.option)) {
         System.out.println(parser.printHelp());
         System.exit(0);
      }

      if (!parser.hasOption(Parameter.JMX_HOSTNAME.option)) {
         System.err.println("Expected " + Parameter.JMX_HOSTNAME.option.getParameter());
         System.err.println(parser.printHelp());
         System.exit(-1);
      }

      new StopJmxRequest(parser.getOption(Parameter.JMX_COMPONENT.option),
                         parser.getOption(Parameter.JMX_HOSTNAME.option),
                         parser.getOption(Parameter.JMX_PORT.option))
            .doRequest();

   }

   public final void doRequest() throws Exception {
      if (benchmarkComponent == null) {
         throw new NullPointerException("Component does not exists");
      }

      mBeanServerConnection.invoke(benchmarkComponent, "stopBenchmark", new Object[0], new String[0]);

      System.out.println("Stop benchmark!");
   }

   private enum Parameter {
      JMX_HOSTNAME("-hostname", false),
      JMX_PORT("-port", false, DEFAULT_JMX_PORT),
      JMX_COMPONENT("-jmx-component", false, DEFAULT_COMPONENT),
      HELP("-help", true);
      private final Option option;

      Parameter(String arg, boolean isBoolean, String defaultValue) {
         option = new Option(arg, null, isBoolean, defaultValue);
      }

      Parameter(String arg, boolean isBoolean) {
         option = new Option(arg, null, isBoolean, null);
      }

      public static Collection<Option> asList() {
         List<Option> optionList = new LinkedList<Option>();
         for (Parameter parameter : values()) {
            optionList.add(parameter.option);
         }
         return optionList;
      }
   }
}
