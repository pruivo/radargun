package org.radargun.cachewrappers;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.keyfeature.NumericFeature;
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

   private static enum RadargunFeature {
      NODE_INDEX("node_index"),
      THREAD_INDEX("thread_index"),
      KEY_INDEX("key_index");

      final String featureName;

      private RadargunFeature(String featureName) {
         this.featureName = featureName;
      }
   }

   private final Feature[] features;

   @SuppressWarnings("UnusedDeclaration") //loaded dynamically
   public RadargunKeyFeatureManager() {
      features = new Feature[RadargunFeature.values().length];
      for (RadargunFeature feature : RadargunFeature.values()) {
         features[feature.ordinal()] = new NumericFeature(feature.featureName);
      }
   }

   @Override
   public Feature[] getAllKeyFeatures() {
      return features;
   }

   @Override
   public Map<Feature, FeatureValue> getFeatures(Object key) {
      if (!(key instanceof String)) {
         return Collections.emptyMap();
      }

      Map<Feature, FeatureValue> featureValueMap = new HashMap<Feature, FeatureValue>();

      String[] split = ((String) key).split(KeyGeneratorFactory.SEPARATOR);

      int index = 1; //starts with a key prefix             
      for (RadargunFeature radargunFeature : RadargunFeature.values()) {
         Feature feature = features[radargunFeature.ordinal()];
         featureValueMap.put(feature, feature.createFeatureValue(Integer.parseInt(split[index++])));
      }

      return featureValueMap;
   }
}
