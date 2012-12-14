package org.radargun;

import org.radargun.parser.ArgumentsParser;
import org.radargun.parser.Option;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * implements a change workload request to radargun
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntWorkloadJmxRequest {

   private enum Parameter {
      //Synthetic Benchmark
      WRT_TX_WRITES("-writeTxWrites", false, "setWriteTransactionWrites", "String"),
      WRT_TX_READS("-writeTxReads", false, "setWriteTransactionReads", "String"),
      RD_TX_READS("-readTxReads", false, "setReadTransactionsReads", "String"),
      WRITE_PERCENTAGE("-writePercentage", false, "setWriteTransactionPercentage", "int"),

      //Bank Benchmark
      TRANSFER_WEIGHT("-transferWeight", false, "setTransferWeight", "int"),
      DEPOSIT_WEIGHT("-depositWeight", false, "setDepositWeight", "int"),
      WITHDRAW_WEIGHT("-withdrawWeight", false, "setWithdrawWeight", "int"),
      NUM_TRANSFERS("-numTransfers", false, "setNumberOfTransfers", "String"),
      NUM_DEPOSIT("-numDeposit", false, "setNumberOfDeposits", "String"),
      NUM_WITHDRAW("-numWithdraw", false, "setNumberOfWithdraws", "String"),

      //Common to synthetic and bank benchmarks
      VALUE_SIZE("-valueSize", false, "setSizeOfValue", "int"),
      NR_THREADS("-nrThreads", false, "setNumberOfThreads", "int"),
      NR_KEYS("-nrKeys", false, "setNumberOfKeys", "int"),
      CONTENTION("-contention", false, "setNoContention", "boolean"),
      LOCALITY_PROBABILITY("-locality-prob", false, "setLocalityProbability", "int"),
      STD_DEV("-std-dev", false, "setStdDev", "int"),

      //TPC-C Benchmark
      //....

      //Configuration
      JMX_COMPONENT("-jmxComponent", false, null, null, DEFAULT_COMPONENT),
      JMX_HOSTNAME("-hostname", false, null, null),
      JMX_PORT("-port", false, null, null, DEFAULT_JMX_PORT),
      HELP("-help", true, null, null);

      private final Option option;
      private final String methodName;
      private final String[] types;

      Parameter(String argumentName, boolean isBoolean, String methodName, String type) {
         this.methodName = methodName;
         this.types = new String[] {type};
         this.option = new Option(argumentName, null, isBoolean, null);
      }

      Parameter(String argumentName, boolean isBoolean, String methodName, String type, String defaultValue) {
         this.methodName = methodName;
         this.types = new String[] {type};
         this.option = new Option(argumentName, null, isBoolean, defaultValue);
      }

      public static Collection<Option> asList() {
         List<Option> optionList = new LinkedList<Option>();
         for (Parameter parameter : values()) {
            optionList.add(parameter.option);
         }
         return optionList;
      }

   }

   private static final String COMPONENT_PREFIX = "org.radargun:stage=";
   private static final String DEFAULT_COMPONENT = "Benchmark";
   private static final String DEFAULT_JMX_PORT = "9998";

   private final ObjectName benchmarkComponent;
   private final MBeanServerConnection mBeanServerConnection;

   private final EnumMap<Parameter, Object> optionValues;

   public static void main(String[] args) throws Exception {
      ArgumentsParser parser = new ArgumentsParser(Parameter.asList());
      parser.parse(args);

      System.out.println("Options are " + parser.printOptions());


      if (parser.hasOption(Parameter.HELP.option)) {
         System.out.println(parser.printHelp());
         System.exit(0);
      }

      SyntWorkloadJmxRequest request = new SyntWorkloadJmxRequest(parser.getOption(Parameter.JMX_COMPONENT.option),
                                                                  parser.getOption(Parameter.JMX_HOSTNAME.option),
                                                                  parser.getOption(Parameter.JMX_PORT.option));

      for (Parameter parameter : Parameter.values()) {
         if (!parser.hasOption(parameter.option)) {
            continue;
         }
         Number number;
         switch (parameter) {
            case WRT_TX_WRITES:
               request.setWriteTxWrites(parser.getOption(parameter.option));
               break;
            case WRT_TX_READS:
               request.setWriteTxReads(parser.getOption(parameter.option));
               break;
            case RD_TX_READS:
               request.setReadTxReads(parser.getOption(parameter.option));
               break;
            case NR_KEYS:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setNumberOfKeys(number.intValue());
               }
               break;
            case NR_THREADS:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setNumberOfThreads(number.intValue());
               }
               break;
            case VALUE_SIZE:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setValueSize(number.intValue());
               }
               break;
            case WRITE_PERCENTAGE:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setWriteTxPercentage(number.intValue());
               }
               break;
            case LOCALITY_PROBABILITY:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setLocalityProbability(number.intValue());
               }
               break;
            case STD_DEV:
               number = parser.getOptionAsNumber(parameter.option);
               if (number != null) {
                  request.setStdDev(number.doubleValue());
               }
               break;
            case CONTENTION:
               request.setContention(true);
               break;
            default:
               //no-op
         }
      }

      request.doRequest();

   }

   private SyntWorkloadJmxRequest(String component, String hostname, String port) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      optionValues = new EnumMap<Parameter, Object>(Parameter.class);
   }

   public void setWriteTxPercentage(int value) {
      if (value >= 0 && value <= 100) {
         optionValues.put(Parameter.WRITE_PERCENTAGE, value);
      }
   }

   public void setWriteTxWrites(String value) {
      if (value == null || value.isEmpty()) {
         return;
      }
      optionValues.put(Parameter.WRT_TX_WRITES, value);
   }

   public void setWriteTxReads(String value) {
      if (value == null || value.isEmpty()) {
         return;
      }
      optionValues.put(Parameter.WRT_TX_READS, value);
   }

   public void setReadTxReads(String value) {
      if (value == null || value.isEmpty()) {
         return;
      }
      optionValues.put(Parameter.RD_TX_READS, value);
   }

   public void setValueSize(int value) {
      if (value > 0) {
         optionValues.put(Parameter.VALUE_SIZE, value);
      }
   }

   public void setNumberOfThreads(int value) {
      if (value > 0) {
         optionValues.put(Parameter.NR_THREADS, value);
      }
   }

   public void setNumberOfKeys(int value) {
      if (value > 0) {
         optionValues.put(Parameter.NR_KEYS, value);
      }
   }

   public void setContention(boolean value) {
      optionValues.put(Parameter.CONTENTION, value);
   }

   public void setLocalityProbability(int value) {
      optionValues.put(Parameter.LOCALITY_PROBABILITY, value);
   }

   public void setStdDev(double value) {
      optionValues.put(Parameter.STD_DEV, value);
   }

   public void doRequest() {
      if (benchmarkComponent == null) {
         throw new NullPointerException("Component does not exists");
      }

      for (Map.Entry<Parameter, Object> entry : optionValues.entrySet()) {
         Parameter parameter = entry.getKey();
         Object value = entry.getValue();
         invoke(parameter, value);
      }

      try {
         mBeanServerConnection.invoke(benchmarkComponent, "changeKeysWorkload", new Object[0], new String[0]);
      } catch (Exception e) {
         System.out.println("Failed to invoke changeKeysWorkload");
      }

      System.out.println("done!");
   }

   private void invoke(Parameter parameter, Object value) {
      System.out.println("Invoking " + parameter.methodName + " for " + parameter.option.getParameter()
                               + " with " + value);
      try {
         if (parameter.option.isFlag()) {
            mBeanServerConnection.invoke(benchmarkComponent, parameter.methodName, new Object[0], new String[0]);
         } else {
            mBeanServerConnection.invoke(benchmarkComponent, parameter.methodName, new Object[] {value}, parameter.types);
         }
      } catch (Exception e) {
         System.out.println("Failed to invoke " + parameter.methodName);
      }
   }

}
