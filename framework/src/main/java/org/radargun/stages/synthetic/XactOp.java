package org.radargun.stages.synthetic;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class XactOp {
   private Object key;
   private Object value;
   private boolean isPut;

   public XactOp(Object key, Object value, boolean put) {
      this.key = key;
      this.value = value;
      isPut = put;
   }

   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   public boolean isPut() {
      return isPut;
   }

   public void setPut(boolean put) {
      isPut = put;
   }

   @Override
   public String toString() {
      return "XactOp{" +
            "key=" + key +
            ", value=" + value +
            ", isPut=" + isPut +
            '}';
   }
}

