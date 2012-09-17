package org.radargun.cachewrappers;

import org.infinispan.dataplacement.keyfeature.AbstractFeature;

import java.util.List;

/**
 * Radargun key feature
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class RadargunKeyFeature extends AbstractFeature {
   
   public RadargunKeyFeature(String name) {
      super(name, FeatureType.NUMBER);
   }
   
   @Override
   public List<String> getListOfNames() {
      return null;
   }
}
