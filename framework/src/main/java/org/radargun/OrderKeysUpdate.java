package org.radargun;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class OrderKeysUpdate {

   public static void main(String[] args) {
      if (args.length == 0) {
         System.err.println("Error. file expected");
         System.exit(1);
      }

      HashMap<String, List<String>> allKeys = new HashMap<String, List<String>>();

      try {
         BufferedReader br = new BufferedReader(new FileReader(args[0]));

         String line;
         //11:08:56,034 TRACE [ReadCommittedEntry] Updating entry (key=KEY_0 removed=false valid=true changed=true created=true value=<value> newVersion=null]
         while ((line = br.readLine()) != null) {
            if (!line.contains("Updating entry")) {
               continue;
            }

            String key = getKey(line);
            String value = getValue(line);
            String version = getVersion(line);

            if (value.length() > 10) {
               value = value.substring(0, 10);
            }

            List<String> list = allKeys.get(key);
            if (list == null) {
               list = new LinkedList<String>();
               allKeys.put(key, list);
            }
            list.add(value + ":" + version);


         }
         br.close();
      } catch (FileNotFoundException e) {
         e.printStackTrace();  // TODO: Customise this generated block
      } catch (IOException e) {
         e.printStackTrace();  // TODO: Customise this generated block
      }

      for (Map.Entry<String, List<String>> entry : allKeys.entrySet()) {
         System.out.print(entry.getKey());
         for (String s : entry.getValue()) {
            System.out.print("|");
            System.out.print(s);
         }
         System.out.println();
      }
   }

   private static String getKey(String key) {
      return getSubstringBetween("key=", " removed=", key);
   }

   private static String getValue(String value) {
      return getSubstringBetween("value=", " newVersion=", value);
   }

   private static String getVersion(String version) {
      String aux = getSubstringBetween("newVersion=", "]", version);
      if (aux != null && aux.contains("version=")) {
         return getSubstringBetween("version=", "}", aux);
      }
      return aux;
   }

   private static String getSubstringBetween(String begin, String end, String line) {
      int beginIdx = begin != null ? line.indexOf(begin) : 0;
      if (beginIdx == -1) {
         return null;
      }
      beginIdx += begin != null ? begin.length() : 0;

      int endIdx = end != null ? line.indexOf(end, beginIdx) : line.length();

      if (endIdx == -1) {
         return null;
      }

      if (beginIdx > endIdx || endIdx > line.length()) {
         return null;
      }

      return line.substring(beginIdx, endIdx);
   }
}
