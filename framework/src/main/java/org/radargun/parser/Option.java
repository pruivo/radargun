package org.radargun.parser;

import java.util.Comparator;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class Option {

   public static final Comparator<Option> COMPARATOR = new Comparator<Option>() {
      @Override
      public int compare(Option option, Option option2) {
         if (option == null) {
            return -1;
         } else if (option2 == null) {
            return 1;
         }
         return option.parameter.compareTo(option2.parameter);
      }
   };
   private static final String NOT_AVAILABLE = "N/A";
   private final String parameter;
   private final String description;
   private final boolean flag;
   private final String defaultValue;

   public Option(String parameter, String description, boolean flag, String defaultValue) {
      if (parameter == null) {
         throw new NullPointerException("Parameter cannot be null");
      }
      this.parameter = parameter;
      this.description = description == null ? NOT_AVAILABLE : description;
      this.flag = flag;
      this.defaultValue = defaultValue;
   }

   public Option(String parameter) {
      this(parameter, null, false, null);
   }

   public String getParameter() {
      return parameter;
   }

   public String getDescription() {
      return description;
   }

   public boolean isFlag() {
      return flag;
   }

   public String getDefaultValue() {
      return defaultValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Option option = (Option) o;

      return !(parameter != null ? !parameter.equals(option.parameter) : option.parameter != null);

   }

   @Override
   public int hashCode() {
      return parameter != null ? parameter.hashCode() : 0;
   }
}
