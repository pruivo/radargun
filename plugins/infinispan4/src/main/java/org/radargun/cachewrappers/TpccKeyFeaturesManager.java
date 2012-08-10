package org.radargun.cachewrappers;

import org.infinispan.dataplacement.keyfeature.AbstractFeature;
import org.infinispan.dataplacement.keyfeature.FeatureValue;
import org.infinispan.dataplacement.keyfeature.KeyFeatureManager;
import org.radargun.tpcc.DomainObject;
import org.radargun.tpcc.domain.Customer;
import org.radargun.tpcc.domain.CustomerLookup;
import org.radargun.tpcc.domain.District;
import org.radargun.tpcc.domain.Item;
import org.radargun.tpcc.domain.NewOrder;
import org.radargun.tpcc.domain.Order;
import org.radargun.tpcc.domain.OrderLine;
import org.radargun.tpcc.domain.Stock;
import org.radargun.tpcc.domain.Warehouse;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Splits the Keys in features and values
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class TpccKeyFeaturesManager implements KeyFeatureManager {

   private static enum Feature {
      WAREHOUSE_ID("warehouse_id"),
      DISTRICT_ID("district_id"),
      CUSTOMER_ID("customer_id"),
      ITEM_ID("item_id"),
      ORDER_ID("order_id"),
      ORDER_LINE_ID("order_line_id");

      final String featureName;

      private Feature(String featureName) {
         this.featureName = featureName;
      }
   }

   private static enum KeyType {
      CUSTOMER(Customer.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID, Feature.CUSTOMER_ID),
      CUSTOMER_LOOKUP(CustomerLookup.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID),
      DISTRICT(District.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID),
      ITEM(Item.KEY_PREFIX, Feature.ITEM_ID),
      NEW_ORDER(NewOrder.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID, Feature.ORDER_ID),
      ORDER(Order.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID, Feature.ORDER_ID),
      ORDER_LINE(OrderLine.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.DISTRICT_ID, Feature.ORDER_ID, Feature.ORDER_LINE_ID),
      STOCK(Stock.KEY_PREFIX, Feature.WAREHOUSE_ID, Feature.ITEM_ID),
      WAREHOUSE(Warehouse.KEY_PREFIX, Feature.WAREHOUSE_ID);

      private final String prefix;
      final Feature[] features;

      private KeyType(String prefix, Feature... features) {
         this.prefix = prefix;
         this.features = features;
      }

      static KeyType fromString(String keyPrefix) {
         for (KeyType keyType : values()) {
            if (keyType.prefix.equals(keyPrefix)) {
               return keyType;
            }
         }
         return null;
      }
   }

   private final Map<Feature, AbstractFeature> featureMap;
   private final AbstractFeature[] features;

   @SuppressWarnings("UnusedDeclaration") //Loaded dynamically
   public TpccKeyFeaturesManager() {
      featureMap = new EnumMap<Feature, AbstractFeature>(Feature.class);

      for (Feature feature : Feature.values()) {
         featureMap.put(feature, new TpccFeature(feature.featureName));
      }

      features = featureMap.values().toArray(new AbstractFeature[featureMap.size()]);
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

      String[] tpccKeysIds = ((String) key).split(DomainObject.ID_SEPARATOR);

      KeyType keyType = KeyType.fromString(tpccKeysIds[0]);

      if (keyType == null) {
         return Collections.emptyMap();
      }

      Map<AbstractFeature, FeatureValue> features = new HashMap<AbstractFeature, FeatureValue>();

      int idx = 1;
      for (Feature feature : keyType.features) {
         set(features, feature, tpccKeysIds[idx++]);
      }

      return features;
   }

   private void set(Map<AbstractFeature, FeatureValue> features, Feature feature, String value) {
      features.put(featureMap.get(feature), featureMap.get(feature).createFeatureValue(Integer.parseInt(value)));

   }

   private class TpccFeature extends AbstractFeature {

      private String featureName;

      private TpccFeature(String featureName) {
         super(FeatureType.NUMBER);
         this.featureName = featureName;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TpccFeature that = (TpccFeature) o;

         return !(featureName != null ? !featureName.equals(that.featureName) : that.featureName != null);

      }

      @Override
      public int hashCode() {
         return featureName != null ? featureName.hashCode() : 0;
      }

      @Override
      public List<String> getListOfNames() {
         return null;
      }

      @Override
      public String getFeatureName() {
         return featureName;
      }
   }
}
