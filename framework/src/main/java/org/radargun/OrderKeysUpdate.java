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
   private static final int KEY_IDX = 5;
   private static final int VALUE_IDX = 10;
   private static final int VERSION_IDX = 11;

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
            String[] split = line.split(" ");

            String key = getKey(split[KEY_IDX]);
            String value = getValue(split[VALUE_IDX]);
            String version = getVersion(split[VERSION_IDX]);

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
      return key.split("=")[1];
   }

   private static String getValue(String value) {
      return value.split("=")[1];
   }

   private static String getVersion(String version) {
      return version.split("=")[1];
   }
}
