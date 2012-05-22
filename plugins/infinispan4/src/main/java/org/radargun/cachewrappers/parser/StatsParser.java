package org.radargun.cachewrappers.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author pedro
 * @since 4.0
 */
public class StatsParser {
   private static final Log log = LogFactory.getLog(StatsParser.class);

   private static final String ROOT_TAG = "stats-collector";
   private static final String COMPONENT_TAG = "component";
   private static final String STAT_TAG = "stat";

   private static final String COMPONENT_NAME = "name";
   private static final String STAT_DISPLAY_NAME = "displayName";
   private static final String STAT_ATTRIBUTE_NAME = "attributeName";


   public static List<StatisticComponent> parse(String file) {
      Document document;
      try {
         DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
         document = builder.parse(file);
      } catch (Exception e) {
         log.error("Cannot parse file " + file + ". No stats will be reported");
         return Collections.emptyList();
      }

      Element root = (Element) document.getElementsByTagName(ROOT_TAG).item(0);

      if (root == null) {
         log.warn("The root tag wasn't found. Expected: "+ ROOT_TAG +". No stats will be reported");
         return Collections.emptyList();
      }

      NodeList components = root.getElementsByTagName(COMPONENT_TAG);

      if (components == null) {
         log.warn("Tag " + COMPONENT_TAG + " wasn't found. No stats will be reported");
         return Collections.emptyList();
      }

      List<StatisticComponent> result = new ArrayList<StatisticComponent>(components.getLength());

      for (int i = 0; i < components.getLength(); ++i) {
         Element component = (Element) components.item(i);
         String componentName = component.getAttribute(COMPONENT_NAME);
         StatisticComponent newComponent = new StatisticComponent(componentName);

         NodeList stats = component.getElementsByTagName(STAT_TAG);
         if (stats == null) {
            log.trace("No stats found for component " + componentName + ". Skipping add it to list");
            continue;
         }
         for (int j = 0; j < stats.getLength(); ++j) {
            Element stat = (Element) stats.item(j);
            String displayName = stat.getAttribute(STAT_DISPLAY_NAME);
            String attributeName = stat.getAttribute(STAT_ATTRIBUTE_NAME);
            newComponent.add(displayName, attributeName);
            log.trace("Added name=" + displayName + " for attribute " + attributeName + " in component " + componentName);
         }
         result.add(newComponent);
      }
      return result;
   }

}
