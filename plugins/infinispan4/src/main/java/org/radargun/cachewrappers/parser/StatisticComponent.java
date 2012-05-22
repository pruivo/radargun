package org.radargun.cachewrappers.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author pedro
 * @since 4.0
 */
public class StatisticComponent {
   private String name;
   private Map<String, String> statsName = new HashMap<String, String>();

   public StatisticComponent(String name) {
      this.name = name;
   }

   public void add(String displayName, String attributeName) {
      statsName.put(displayName, attributeName);
   }

   public Set<Map.Entry<String, String>> getStats() {
      return statsName.entrySet();
   }

   public String getName() {
      return name;
   }

   @Override
   public String toString() {
      return "StatisticComponent{" +
            "name='" + name + '\'' +
            ", statsName=" + statsName +
            '}';
   }
}
