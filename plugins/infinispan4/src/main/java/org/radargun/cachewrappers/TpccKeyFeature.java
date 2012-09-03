package org.radargun.cachewrappers;

import org.infinispan.dataplacement.keyfeature.AbstractFeature;

import java.util.List;

/**
 * Represents a key feature
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class TpccKeyFeature extends AbstractFeature {

   public TpccKeyFeature(String featureName) {
      super(featureName, FeatureType.NUMBER);
   }

   @Override
   public List<String> getListOfNames() {
      return null;
   }
}