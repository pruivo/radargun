package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.DomainObject;

import java.util.Date;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class Order extends DomainObject<Order> implements Comparable {

   private long o_id;

   private long o_d_id;

   private long o_w_id;

   private long o_c_id;

   private long o_entry_d;

   private long o_carrier_id;

   private int o_ol_cnt;

   private int o_all_local;


   public Order() {

   }

   public Order(long o_id, long o_d_id, long o_w_id, long o_c_id, Date o_entry_d, long o_carrier_id, int o_ol_cnt, int o_all_local) {
      this.o_id = o_id;
      this.o_d_id = o_d_id;
      this.o_w_id = o_w_id;
      this.o_c_id = o_c_id;
      this.o_entry_d = (o_entry_d == null) ? -1 : o_entry_d.getTime();
      this.o_carrier_id = o_carrier_id;
      this.o_ol_cnt = o_ol_cnt;
      this.o_all_local = o_all_local;
   }

   public long getO_id() {
      return o_id;
   }

   public long getO_d_id() {
      return o_d_id;
   }

   public long getO_w_id() {
      return o_w_id;
   }

   public long getO_c_id() {
      return o_c_id;
   }

   public Date getO_entry_d() {
      return o_entry_d == -1 ? null : new Date(o_entry_d);
   }

   public long getO_carrier_id() {
      return o_carrier_id;
   }

   public int getO_ol_cnt() {
      return o_ol_cnt;
   }

   public int getO_all_local() {
      return o_all_local;
   }

   public void setO_id(long o_id) {
      this.o_id = o_id;
   }

   public void setO_d_id(long o_d_id) {
      this.o_d_id = o_d_id;
   }

   public void setO_w_id(long o_w_id) {
      this.o_w_id = o_w_id;
   }

   public void setO_c_id(long o_c_id) {
      this.o_c_id = o_c_id;
   }

   public void setO_entry_d(Date o_entry_d) {
      this.o_entry_d = (o_entry_d == null) ? -1 : o_entry_d.getTime();
   }

   public void setO_carrier_id(long o_carrier_id) {
      this.o_carrier_id = o_carrier_id;
   }

   public void setO_ol_cnt(int o_ol_cnt) {
      this.o_ol_cnt = o_ol_cnt;
   }

   public void setO_all_local(int o_all_local) {
      this.o_all_local = o_all_local;
   }

   //For a decreasing order in sort operation
   @Override
   public int compareTo(Object o) {
      if (o == null || !(o instanceof Order)) return -1;

      Order other = (Order) o;

      if (this.o_id == other.o_id) return 0;
      else if (this.o_id > other.o_id) return -1;
      else return 1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Order order = (Order) o;

      if (o_all_local != order.o_all_local) return false;
      if (o_c_id != order.o_c_id) return false;
      if (o_carrier_id != order.o_carrier_id) return false;
      if (o_d_id != order.o_d_id) return false;
      if (o_entry_d != order.o_entry_d) return false;
      if (o_id != order.o_id) return false;
      if (o_ol_cnt != order.o_ol_cnt) return false;
      if (o_w_id != order.o_w_id) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (o_id ^ (o_id >>> 32));
      result = 31 * result + (int) (o_d_id ^ (o_d_id >>> 32));
      result = 31 * result + (int) (o_w_id ^ (o_w_id >>> 32));
      result = 31 * result + (int) (o_c_id ^ (o_c_id >>> 32));
      result = 31 * result + (int) (o_entry_d ^ (o_entry_d >>> 32));
      result = 31 * result + (int) (o_carrier_id ^ (o_carrier_id >>> 32));
      result = 31 * result + o_ol_cnt;
      result = 31 * result + o_all_local;
      return result;
   }

   @Override
   public boolean load(CacheWrapper wrapper) throws Throwable {

      Order loaded = internalLoad(wrapper);

      if (loaded == null) return false;


      this.o_c_id = loaded.o_c_id;
      this.o_carrier_id = loaded.o_carrier_id;
      this.o_entry_d = loaded.o_entry_d;
      this.o_ol_cnt = loaded.o_ol_cnt;
      this.o_all_local = loaded.o_all_local;


      return true;
   }

   @Override
   protected TpccKey createTpccKey() {
      return new OrderKey(o_id, o_d_id, o_w_id);
   }

   public static class OrderKey extends TpccKey {

      private final long orderId;
      private final long districtId;
      private final long warehouseId;

      public OrderKey(long orderId, long districtId, long warehouseId) {
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
         return "OrderKey{" +
               "orderId=" + orderId +
               ", districtId=" + districtId +
               ", warehouseId=" + warehouseId +
               '}';
      }
   }
}
