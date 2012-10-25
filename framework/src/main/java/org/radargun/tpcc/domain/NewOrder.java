package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.DomainObject;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class NewOrder extends DomainObject<NewOrder> {

   private long no_o_id;

   private long no_d_id;

   private long no_w_id;

   public NewOrder() {}

   public NewOrder(long no_o_id, long no_d_id, long no_w_id) {
      this.no_o_id = no_o_id;
      this.no_d_id = no_d_id;
      this.no_w_id = no_w_id;
   }


   public long getNo_o_id() {
      return no_o_id;
   }

   public long getNo_d_id() {
      return no_d_id;
   }

   public long getNo_w_id() {
      return no_w_id;
   }

   public void setNo_o_id(long no_o_id) {
      this.no_o_id = no_o_id;
   }

   public void setNo_d_id(long no_d_id) {
      this.no_d_id = no_d_id;
   }

   public void setNo_w_id(long no_w_id) {
      this.no_w_id = no_w_id;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NewOrder newOrder = (NewOrder) o;

      if (no_d_id != newOrder.no_d_id) return false;
      if (no_o_id != newOrder.no_o_id) return false;
      if (no_w_id != newOrder.no_w_id) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (no_o_id ^ (no_o_id >>> 32));
      result = 31 * result + (int) (no_d_id ^ (no_d_id >>> 32));
      result = 31 * result + (int) (no_w_id ^ (no_w_id >>> 32));
      return result;
   }

   @Override
   public boolean load(CacheWrapper wrapper) throws Throwable {
      return true;
   }

   @Override
   protected TpccKey createTpccKey() {
      return new NewOrderKey(no_o_id, no_d_id, no_w_id);
   }

   public static class NewOrderKey extends TpccKey {

      private final long orderId;
      private final long districtId;
      private final long warehouseId;

      public NewOrderKey(long orderId, long districtId, long warehouseId) {
         this.orderId = orderId;
         this.districtId = districtId;
         this.warehouseId = warehouseId;
      }

      @Override
      public Number getWarehouseId() {
         return warehouseId;
      }

      @Override
      public Number getDistrictId() {
         return districtId;
      }

      @Override
      public Number getOrderId() {
         return orderId;
      }

      @Override
      public String toString() {
         return "NewOrderKey{" +
               "orderId=" + orderId +
               ", districtId=" + districtId +
               ", warehouseId=" + warehouseId +
               '}';
      }
   }
}
