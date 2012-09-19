package org.radargun.cachewrappers;

import org.infinispan.dataplacement.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.keyfeature.FeatureValue;
import org.infinispan.dataplacement.keyfeature.KeyFeatureManager;
import org.radargun.keygen2.KeyGeneratorFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Key Features manager for Radargun. Used by the data placement algorithm
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class RadargunKeyFeatureManager implements KeyFeatureManager {

   private static enum Feature {
      NODE_INDEX("node_index"),
      THREAD_INDEX("thread_index"),
      KEY_INDEX("key_index");

      final String featureName;

      private Feature(String featureName) {
         this.featureName = featureName;
      }
   }

   private final AbstractFeature[] features;

   @SuppressWarnings("UnusedDeclaration") //loaded dynamically
   public RadargunKeyFeatureManager() {
      features = new AbstractFeature[Feature.values().length];
      for (Feature feature : Feature.values()) {
         features[feature.ordinal()] = new RadargunKeyFeature(feature.featureName);
      }
   }

   @Override
   public AbstractFeature[] getAllKeyFeatures() {
      return features;
   }

   @Override
   public Map<AbstractFeature, FeatureValue> getFeatures(Object key) {
      if (!(key instanceof String)) {
         return Collections.emptyMap();
      }

      Map<AbstractFeature, FeatureValue> featureValueMap = new HashMap<AbstractFeature, FeatureValue>();

      String[] split = ((String) key).split(KeyGeneratorFactory.SEPARATOR);

      int index = 1; //starts with a key prefix             
      for (Feature feature : Feature.values()) {
         AbstractFeature abstractFeature = features[feature.ordinal()];
         featureValueMap.put(abstractFeature, abstractFeature.createFeatureValue(Integer.parseInt(split[index++])));
      }

      return featureValueMap;
   }
}
