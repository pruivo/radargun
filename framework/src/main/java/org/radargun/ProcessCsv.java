package org.radargun;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class ProcessCsv {

   private enum Option {
      SUM("-sum", false),
      AVG("-avg", false),
      HELP("-help", true);

      private final String argumentName;
      private final boolean isBoolean;

      Option(String argumentName, boolean aBoolean) {
         isBoolean = aBoolean;
         if (argumentName == null) {
            throw new IllegalArgumentException("Null not allowed");
         }
         this.argumentName = argumentName;
      }

      public final String toString() {
         return argumentName;
      }

      public static Option fromString(String optionName) {
         if (optionName == null) {
            return null;
         }
         for (Option option : values()) {
            if (optionName.startsWith(option.argumentName)) {
               return option;
            }
         }
         return null;
      }
   }


   public static void main(String[] args) throws InterruptedException {
      Arguments arguments = new Arguments();
      arguments.parse(args);
      arguments.validate();

      if (arguments.hasOption(Option.HELP)) {
         System.out.println("usage: java " + ProcessCsv.class.getSimpleName() + " -sum=attr1,...,attrN -avg=attr1,...,attrN file1 ... fileN");
         System.exit(0);
      }

      String[] sumArray = arguments.hasOption(Option.SUM) ? arguments.getValue(Option.SUM).split(",") : new String[0];
      String[] avgArray = arguments.hasOption(Option.AVG) ? arguments.getValue(Option.AVG).split(",") : new String[0];
      List<CsvFileProcessor> processorList = new LinkedList<CsvFileProcessor>();

      for (String filePath : arguments.getFilePaths()) {
         processorList.add(new CsvFileProcessor(getCsvHeaders(sumArray, avgArray), filePath));
      }

      ExecutorService executor = Executors.newFixedThreadPool(4);
      List<Future<Object>> futureList = executor.invokeAll(processorList);

      for (Future<Object> future : futureList) {
         try {
            future.get();
         } catch (ExecutionException e) {
            e.printStackTrace();
         }
      }
      executor.shutdown();
      System.exit(0);
   }

   private static CsvHeaderResult[] getCsvHeaders(String[] sumArray, String[] avgArray) {
      CsvHeaderResult[] array = new CsvHeaderResult[sumArray.length + avgArray.length];

      int i = 0;
      for (String s : sumArray) {
         array[i++] = new SumCsvHeaderResult(s);
      }
      for (String s : avgArray) {
         array[i++] = new AvgCsvHeaderResult(s);
      }

      return array;
   }

   private static BufferedReader getReader(String filePath) {
      try {
         return new BufferedReader(new FileReader(filePath));
      } catch (FileNotFoundException e) {
         //no-op
      }
      return null;
   }

   private static void close(Closeable closeable) {
      try {
         closeable.close();
      } catch (IOException e) {
         //no-op
      }
   }

   private static class CsvFileProcessor implements Callable<Object>, Runnable {

      private final int[] positions;
      private final CsvHeaderResult[] results;
      private final String filePath;

      private CsvFileProcessor(CsvHeaderResult[] results, String filePath) {
         this.filePath = filePath;
         this.positions = new int[results.length];
         this.results = results;

         for (int i = 0; i < positions.length; ++i) {
            positions[i] = -1;
         }
      }

      @Override
      public void run() {
         BufferedReader reader = getReader(filePath);

         if (reader == null) {
            System.err.println("File " + filePath + " not found!");
            return;
         }

         if (!populateHeaders(reader)) {
            System.err.println("File " + filePath + ": headers not found!");
            close(reader);
            return;
         }

         if (!process(reader)) {
            System.err.println("File " + filePath + ": error processing lines!");
            close(reader);
            return;
         }

         close(reader);
         printResults();
      }

      private boolean populateHeaders(BufferedReader reader) {
         String headers;
         try {
            headers = reader.readLine();
         } catch (IOException e) {
            return false;
         }
         if (headers == null) {
            return false;
         }
         String[] headerArray = headers.split(",");

         for (int i = 0; i < results.length; ++i) {
            CsvHeaderResult result = results[i];
            for (int j = 0; j < headerArray.length; ++j) {
               if (result.header.equals(headerArray[j])) {
                  positions[i] = j;
                  break;
               }
            }
         }
         return true;
      }

      private boolean process(BufferedReader reader) {
         String line;

         try {
            while ((line = reader.readLine()) != null) {
               String[] values = line.split(",");

               for (int i = 0; i < results.length; ++i) {
                  int index = positions[i];

                  if (index < 0) {
                     continue;
                  }

                  try {
                     Number number = NumberFormat.getNumberInstance().parse(values[index]);
                     results[i].add(number.doubleValue());
                  } catch (ParseException e) {
                     //no-op
                  }
               }
            }
         } catch (IOException e) {
            return false;
         }

         return true;
      }

      private void printResults() {
         StringBuilder stringBuilder = new StringBuilder();
         stringBuilder.append("File: ").append(filePath).append("\n");

         for (CsvHeaderResult result : results) {
            stringBuilder.append(result.header).append("=").append(result.get()).append("\n");
         }

         System.out.println(stringBuilder);
      }

      @Override
      public Object call() throws Exception {
         run();
         return null;
      }
   }


   private static abstract class CsvHeaderResult {

      protected final String header;
      protected double sum;

      protected CsvHeaderResult(String header) {
         this.header = header;
         sum = 0;
      }

      public void add(double value) {
         if (value > 0) {
            sum += value;
         }
      }

      abstract double get();
   }

   private static class SumCsvHeaderResult extends CsvHeaderResult {

      protected SumCsvHeaderResult(String header) {
         super(header);
      }

      @Override
      double get() {
         return sum;
      }
   }

   private static class AvgCsvHeaderResult extends CsvHeaderResult {

      private int counter;

      protected AvgCsvHeaderResult(String header) {
         super(header);
         counter = 0;
      }

      @Override
      public void add(double value) {
         if (value >= 0) {
            counter++;
         }
         super.add(value);
      }

      @Override
      double get() {
         return counter > 0 ? sum / counter : -1;
      }
   }

   private static class Arguments {

      private static final String SEPARATOR = "=";

      private final Map<Option, String> argsValues;
      private final List<String> filePaths;

      private Arguments() {
         argsValues = new EnumMap<Option, String>(Option.class);
         filePaths = new LinkedList<String>();
      }

      public final void parse(String[] args) {
         int idx = 0;
         while (idx < args.length) {
            Option option = Option.fromString(args[idx]);
            if (option == null) {
               filePaths.add(args[idx]);
            } else {
               if (option.isBoolean) {
                  argsValues.put(option, Boolean.toString(true));
               } else {
                  argsValues.put(option, args[idx].split(SEPARATOR, 2)[1]);
               }
            }
            idx++;
         }
      }

      public final void validate() {
         //no-op
      }

      public final String getValue(Option option) {
         return argsValues.get(option);
      }

      public final int getValueAsInt(Option option) {
         return Integer.parseInt(argsValues.get(option));
      }

      public final boolean getValueAsBoolean(Option option) {
         return Boolean.parseBoolean(argsValues.get(option));
      }

      public final boolean hasOption(Option option) {
         return argsValues.containsKey(option);
      }

      public final String printOptions() {
         return argsValues.toString();
      }

      public final List<String> getFilePaths() {
         return filePaths;
      }

      @Override
      public final String toString() {
         return "Arguments{" +
               "argsValues=" + argsValues +
               '}';
      }
   }
}
