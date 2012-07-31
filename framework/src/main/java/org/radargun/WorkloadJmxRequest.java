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
      OP_LOWER_BOUND("-lower-bound", false),
      OP_UPPER_BOUND("-upper-bound", false),
      WRITE_PERCENTAGE("-write-percentage", false),
      OP_WRITE_PERCENTAGE("-op-write-percentage", false),
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
   private final int opLowerBound;
   private final int opUpperBound;
   private final int opWritePercentage;
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
                             arguments.getValueAsInt(Option.OP_LOWER_BOUND),
                             arguments.getValueAsInt(Option.OP_UPPER_BOUND),
                             arguments.getValueAsInt(Option.OP_WRITE_PERCENTAGE),
                             arguments.getValueAsInt(Option.WRITE_PERCENTAGE),
                             arguments.getValueAsInt(Option.NR_KEYS),
                             arguments.hasOption(Option.STOP))
            .doRequest();


   }

   private WorkloadJmxRequest(String component, String hostname, String port, int opLowerBound, int opUpperBound,
                              int opWritePercentage, int txWritePercentage, int numberOfKeys, boolean stop) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      this.opLowerBound = opLowerBound;
      this.opUpperBound = opUpperBound;
      this.opWritePercentage = opWritePercentage;
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

      String[] args = new String[] {"int"};

      if (opLowerBound != DON_NOT_MODIFY && opLowerBound > 0) {
         mBeanServerConnection.invoke(benchmarkComponent, "setLowerBoundOp", new Object[] {opLowerBound}, args);
      }
      if (opUpperBound != DON_NOT_MODIFY && opUpperBound > 0) {
         mBeanServerConnection.invoke(benchmarkComponent, "setUpperBoundOp", new Object[] {opUpperBound}, args);
      }
      if (opWritePercentage != DON_NOT_MODIFY && opWritePercentage >= 0 && opWritePercentage <= 100) {
         mBeanServerConnection.invoke(benchmarkComponent, "setWriteOperationPercentage",
                                      new Object[] {opWritePercentage}, args);
      }
      if (txWritePercentage != DON_NOT_MODIFY && txWritePercentage >= 0 && txWritePercentage <= 100) {
         mBeanServerConnection.invoke(benchmarkComponent, "setWriteTransactionPercentage",
                                      new Object[] {txWritePercentage}, args);
      }
      if (numberOfKeys != DON_NOT_MODIFY && numberOfKeys > 0) {
         mBeanServerConnection.invoke(benchmarkComponent, "setNumberOfKeys",
                                      new Object[] {numberOfKeys}, args);
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

         argsValues.put(Option.OP_LOWER_BOUND, doNotChange);
         argsValues.put(Option.OP_UPPER_BOUND, doNotChange);
         argsValues.put(Option.OP_WRITE_PERCENTAGE, doNotChange);
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

         int value = getValueAsInt(Option.OP_LOWER_BOUND);
         if (value != DON_NOT_MODIFY && value < 1) {
            throw new IllegalArgumentException("Option " + Option.OP_LOWER_BOUND + " must be higher than zero. "
                                                     + "Value is " + value);
         }

         value = getValueAsInt(Option.OP_UPPER_BOUND);
         if (value != DON_NOT_MODIFY && value < 1) {
            throw new IllegalArgumentException("Option " + Option.OP_UPPER_BOUND + " must be higher than zero. "
                                                     + "Value is " + value);
         }

         value = getValueAsInt(Option.OP_WRITE_PERCENTAGE);
         if (value != DON_NOT_MODIFY && (value < 0 || value > 100)) {
            throw new IllegalArgumentException("Option " + Option.OP_WRITE_PERCENTAGE + " must be higher or equals " +
                                                     "than zero " +
                                                     " and less or equals than 100. Value is " + value);
         }

         value = getValueAsInt(Option.WRITE_PERCENTAGE);
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
