package org.radargun.cachewrappers;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.keyfeature.NumericFeature;
import org.radargun.tpcc.domain.TpccKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Splits the Keys in features and values
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class TpccKeyFeaturesManager implements KeyFeatureManager {

    private static enum TpccFeature {
        WAREHOUSE_ID("warehouse_id"),
        DISTRICT_ID("district_id"),
        CUSTOMER_ID("customer_id"),
        ITEM_ID("item_id"),
        ORDER_ID("order_id"),
        ORDER_LINE_ID("order_line_id");

        final Feature feature;

        private TpccFeature(String featureName) {
            this.feature = new NumericFeature(featureName);
        }
    }

    private final Feature[] features;

    public TpccKeyFeaturesManager() {
        features = new Feature[TpccFeature.values().length];
        int idx = 0;
        for (TpccFeature tpccFeature : TpccFeature.values()) {
            features[idx++] = tpccFeature.feature;
        }
    }

    @Override
    public Feature[] getAllKeyFeatures() {
        return features;
    }

    @Override
    public Map<Feature, FeatureValue> getFeatures(Object key) {
        if (key instanceof TpccKey) {
            Map<Feature, FeatureValue> featureValueMap = new HashMap<Feature, FeatureValue>();
            TpccKey tpccKey = (TpccKey) key;
            for (TpccFeature tpccFeature : TpccFeature.values()) {
                add(featureValueMap, tpccFeature, tpccKey);
            }
            return featureValueMap;
        }
        return Collections.emptyMap();
    }

    private void add(Map<Feature, FeatureValue> map, TpccFeature tpccFeature, TpccKey key) {
        Feature feature = tpccFeature.feature;
        Number featureValue = valueOf(tpccFeature, key);
        if (featureValue != null) {
            map.put(feature, feature.createFeatureValue(featureValue));
        }
    }

    private Number valueOf(TpccFeature tpccFeature, TpccKey key) {
        switch (tpccFeature) {
            case WAREHOUSE_ID:
                return key.getWarehouseId();
            case DISTRICT_ID:
                return key.getDistrictId();
            case CUSTOMER_ID:
                return key.getCustomerId();
            case ITEM_ID:
                return key.getItemId();
            case ORDER_ID:
                return key.getOrderId();
            case ORDER_LINE_ID:
                return key.getOrderLineId();
        }
        return null;
    }
}
