package org.radargun.parser;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class ArgumentsParser {

   private final Option[] options;
   private final String[] values;

   public ArgumentsParser(Collection<Option> optionCollection) {
      Set<Option> optionSet = new HashSet<Option>(optionCollection);
      this.options = optionSet.toArray(new Option[optionSet.size()]);
      this.values = new String[optionCollection.size()];
      Arrays.sort(this.options, Option.COMPARATOR);
      for (int i = 0; i < this.options.length; ++i) {
         values[i] = this.options[i].getDefaultValue();
      }
   }

   public final void parse(String[] args) {
      int idx = 0;
      while (idx < args.length) {
         int optionIndex = findOption(args[idx]);

         if (optionIndex < 0) {
            throw new IllegalArgumentException("unkown option: " + args[idx] + ". Possible options are: " +
                                                     optionsToString());
         }
         idx++;
         if (options[optionIndex].isFlag()) {
            values[optionIndex] = Boolean.TRUE.toString();
            continue;
         }
         if (idx >= args.length) {
            throw new IllegalArgumentException("expected a value for option " + options[optionIndex].getParameter());
         }
         values[optionIndex] = args[idx++];
      }
   }

   public final boolean hasOption(String parameter) {
      return hasOption(new Option(parameter));
   }

   public final boolean hasOption(Option option) {
      int idx = findOption(option);
      if (idx < 0) {
         return false;
      }
      if (values[idx] == null) {
         return false;
      } else if (options[idx].isFlag()) {
         return Boolean.valueOf(values[idx]);
      }
      return true;
   }

   public final String getOption(String parameter) {
      return getOption(new Option(parameter));
   }

   public final String getOption(Option option) {
      int idx = findOption(option);
      return idx < 0 ? null : values[idx];
   }

   public final Number getOptionAsNumber(String parameter) {
      return getOptionAsNumber(new Option(parameter));
   }

   public final Number getOptionAsNumber(Option option) {
      int idx = findOption(option);
      if (idx < 0) {
         return null;
      }
      try {
         return NumberFormat.getNumberInstance().parse(values[idx]);
      } catch (ParseException e) {
         //no-op
      }
      return null;
   }

   public final String printHelp() {
      StringBuilder stringBuilder = new StringBuilder(4096);
      for (Option option : options) {
         stringBuilder.append(" ").append(option.getParameter());
         if (!option.isFlag()) {
            stringBuilder.append(" <value>");
         }
         stringBuilder.append(" ").append(option.getDescription());
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
   }

   public final String printOptions() {
      StringBuilder stringBuilder = new StringBuilder(4096);
      for (int i = 0; i < options.length; ++i) {
         stringBuilder.append(options[i].getParameter()).append("='").append(values[i]).append("'\n");
      }
      return stringBuilder.toString();
   }

   private int findOption(String parameter) {
      return findOption(new Option(parameter));
   }

   private int findOption(Option option) {
      return Arrays.binarySearch(options, option);
   }

   private String optionsToString() {
      StringBuilder stringBuilder = new StringBuilder(4096);
      for (Option option : options) {
         stringBuilder.append(option.getParameter()).append(" ");
      }
      return stringBuilder.toString();
   }
}
