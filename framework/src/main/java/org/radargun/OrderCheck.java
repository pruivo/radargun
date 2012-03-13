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
public class OrderCheck {
   public static void main(String[] args) {
      if (args.length == 0) {
         System.err.println("Error. file expected");
         System.exit(1);
      }

      HashMap<String, List<String>> allKeys = new HashMap<String, List<String>>();

      try {
         BufferedReader br = new BufferedReader(new FileReader(args[0]));

         String line;

         while ((line = br.readLine()) != null) {
            String[] split = line.split("[|]");

            String key = split[0];
            String value = split[1];
            String version = split[2];

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
}
