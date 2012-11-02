package org.radargun.tpcc.domain;

import java.io.Serializable;

/**
 * Interface to implement for the TPC-C keys
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class TpccKey implements Serializable {

   public Number getWarehouseId() {
      return null;
   }

   public Number getDistrictId() {
      return null;
   }

   public Number getCustomerId() {
      return null;
   }

   public Number getItemId() {
      return null;
   }

   public Number getOrderId() {
      return null;
   }

   public Number getOrderLineId() {
      return null;
   }}
