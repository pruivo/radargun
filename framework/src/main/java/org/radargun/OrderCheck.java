package org.radargun;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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

      HashMap<String, StringBuilder> allKeys = new HashMap<String, StringBuilder>();

      try {
         BufferedReader br = new BufferedReader(new FileReader(args[0]));

         String line;

         while ((line = br.readLine()) != null) {
            if (line.startsWith("++")) {
               String key = getKey(line);
               String value = getValue(line);

               StringBuilder sb = allKeys.get(key);
               if (sb == null) {
                  sb = new StringBuilder(key).append("=").append(value);
                  allKeys.put(key, sb);
               } else {
                  sb.append(",").append(value);
               }
            }
         }
         br.close();
      } catch (FileNotFoundException e) {
         e.printStackTrace();  // TODO: Customise this generated block
      } catch (IOException e) {
         e.printStackTrace();  // TODO: Customise this generated block
      }

      for (StringBuilder sb : allKeys.values()) {
         System.out.println(sb.toString());
      }
   }

   private static String getValue(String line) {
      String[] split1 = line.split("=");
      if (split1.length < 2) {
         return "";
      }
      return split1[1];
   }

   private static String getKey(String line) {
      String[] split1 = line.split("=");
      if (split1.length <= 0) {
         return "";
      }
      String[] split2 = split1[0].split(" ");
      if (split2.length < 3) {
         return "";
      }
      return split2[2];
   }
}
