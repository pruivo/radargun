package org.radargun;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * implements a change workload request to radargun
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class WorkloadJmxRequest {

   public static final int DON_NOT_MODIFY = -1;

   private enum Option {
      WRT_OP_WRT_TX("-wrt-op-wrt-tx", false),
      RD_OP_WRT_TX("-rd-op-wrt-tx", false),
      RD_OP_RD_TX("-rd-op-rd-tx", false),
      WRITE_PERCENTAGE("-write-percentage", false),
      NR_KEYS("-nr-keys", false),
      STOP("-stop", true),
      JMX_COMPONENT("-jmx-component", false),
      JMX_HOSTNAME("-hostname", false),
      JMX_PORT("-port", false);

      private final String arg;
      private final boolean isBoolean;

      Option(String arg, boolean isBoolean) {
         if (arg == null) {
            throw new IllegalArgumentException("Null not allowed in Option name");
         }
         this.arg = arg;
         this.isBoolean = isBoolean;
      }

      public final String getArgName() {
         return arg;
      }

      public final boolean isBoolean() {
         return isBoolean;
      }

      public final String toString() {
         return arg;
      }

      public static Option fromString(String optionName) {
         for (Option option : values()) {
            if (option.getArgName().equalsIgnoreCase(optionName)) {
               return option;
            }
         }
         return null;
      }
   }

   private static final String COMPONENT_PREFIX = "org.radargun:stage=";
   private static final String DEFAULT_COMPONENT = "BenchmarkStage";
   private static final String DEFAULT_JMX_PORT = "9998";

   private final ObjectName benchmarkComponent;
   private final MBeanServerConnection mBeanServerConnection;
   private final String wrtOpsWrtTx;
   private final String rdOpsWrtTx;
   private final String rdOpsRdTx;
   private final int txWritePercentage;
   private final int numberOfKeys;
   private final boolean stop;

   public static void main(String[] args) throws Exception {
      Arguments arguments = new Arguments();
      arguments.parse(args);
      arguments.validate();

      System.out.println("Options are " + arguments.printOptions());

      new WorkloadJmxRequest(arguments.getValue(Option.JMX_COMPONENT),
                             arguments.getValue(Option.JMX_HOSTNAME),
                             arguments.getValue(Option.JMX_PORT),
                             arguments.getValue(Option.WRT_OP_WRT_TX),
                             arguments.getValue(Option.RD_OP_WRT_TX),
                             arguments.getValue(Option.RD_OP_RD_TX),
                             arguments.getValueAsInt(Option.WRITE_PERCENTAGE),
                             arguments.getValueAsInt(Option.NR_KEYS),
                             arguments.hasOption(Option.STOP))
            .doRequest();


   }

   private WorkloadJmxRequest(String component, String hostname, String port, String wrtOps, String rdWrtTx,
                              String rdRdTx, int txWritePercentage, int numberOfKeys, boolean stop) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      this.wrtOpsWrtTx = wrtOps;
      this.rdOpsRdTx = rdRdTx;
      this.rdOpsWrtTx = rdWrtTx;
      this.txWritePercentage = txWritePercentage;
      this.numberOfKeys = numberOfKeys;
      this.stop = stop;
   }

   public void doRequest() throws Exception {
      if (benchmarkComponent == null) {
         throw new NullPointerException("Component does not exists");
      }

      if (stop) {
         mBeanServerConnection.invoke(benchmarkComponent, "stop", new Object[0], new String[0]);
         return;
      }

      String[] intArg = new String[] {"int"};
      String[] stringArg = new String[] {"String"};

      if (wrtOpsWrtTx != null) {
         mBeanServerConnection.invoke(benchmarkComponent, "setWrtOpsPerWriteTx", new Object[] {wrtOpsWrtTx}, stringArg);
      }
      if (rdOpsWrtTx != null) {
         mBeanServerConnection.invoke(benchmarkComponent, "setRdOpsPerWriteTx", new Object[] {rdOpsWrtTx}, stringArg);
      }
      if (rdOpsRdTx != null) {
         mBeanServerConnection.invoke(benchmarkComponent, "setRdOpsPerReadTx", new Object[] {rdOpsRdTx}, stringArg);
      }
      if (txWritePercentage != DON_NOT_MODIFY && txWritePercentage >= 0 && txWritePercentage <= 100) {
         mBeanServerConnection.invoke(benchmarkComponent, "setWriteTransactionPercentage",
                                      new Object[] {txWritePercentage}, intArg);
      }
      if (numberOfKeys != DON_NOT_MODIFY && numberOfKeys > 0) {
         mBeanServerConnection.invoke(benchmarkComponent, "setNumberOfKeys",
                                      new Object[] {numberOfKeys}, intArg);
      }
      System.out.println("done!");
   }

   private static class Arguments {

      private final Map<Option, String> argsValues;

      private Arguments() {
         argsValues = new EnumMap<Option, String>(Option.class);

         //set the default values
         argsValues.put(Option.JMX_COMPONENT, DEFAULT_COMPONENT);
         argsValues.put(Option.JMX_PORT, DEFAULT_JMX_PORT);

         String doNotChange = Integer.toString(DON_NOT_MODIFY);

         argsValues.put(Option.WRITE_PERCENTAGE, doNotChange);
         argsValues.put(Option.NR_KEYS, doNotChange);
      }

      public final void parse(String[] args) {
         int idx = 0;
         while (idx < args.length) {
            Option option = Option.fromString(args[idx]);
            if (option == null) {
               throw new IllegalArgumentException("unkown option: " + args[idx] + ". Possible options are: " +
                                                        Arrays.asList(Option.values()));
            }
            idx++;
            if (option.isBoolean()) {
               argsValues.put(option, "true");
               continue;
            }
            if (idx >= args.length) {
               throw new IllegalArgumentException("expected a value for option " + option);
            }
            argsValues.put(option, args[idx++]);
         }
      }

      public final void validate() {
         if (!hasOption(Option.JMX_HOSTNAME)) {
            throw new IllegalArgumentException("Option " + Option.JMX_HOSTNAME + " is required");
         }

         int value = getValueAsInt(Option.WRITE_PERCENTAGE);
         if (value != DON_NOT_MODIFY && (value < 0 || value > 100)) {
            throw new IllegalArgumentException("Option " + Option.WRITE_PERCENTAGE + " must be higher or equals than " +
                                                     "zero and less or equals than 100. Value is " + value);
         }

         value = getValueAsInt(Option.NR_KEYS);
         if (value != DON_NOT_MODIFY && value <= 0) {
            throw new IllegalArgumentException("Option " + Option.NR_KEYS + " must be higher than " +
                                                     "zero. Value is " + value);
         }
      }

      public final String getValue(Option option) {
         return argsValues.get(option);
      }

      public final int getValueAsInt(Option option) {
         return Integer.parseInt(argsValues.get(option));
      }

      public final boolean hasOption(Option option) {
         return argsValues.containsKey(option);
      }

      public final String printOptions() {
         return argsValues.toString();
      }

      @Override
      public final String toString() {
         return "Arguments{" +
               "argsValues=" + argsValues +
               '}';
      }
   }

}
