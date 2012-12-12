package org.radargun.workloads;

import org.radargun.CacheWrapper;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The abstract transaction workload factory to be extended for each workload
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class TransactionWorkloadFactory<T extends AbstractTransactionWorkload> {

   private static final int NOT_VALID = -1;
   private static final ParserPattern[] PATTERNS = new ParserPattern[] {
         new ParserPattern("(\\d+),(\\d+)", new int[]{1,2}),
         new ParserPattern("(\\d+)", new int[]{1}),
   };

   public abstract T chooseTransaction(Random random, CacheWrapper wrapper);

   public abstract void calculate();

   protected final int[] parse(String workload) {
      for (ParserPattern parserPattern : PATTERNS) {
         int[] parsedValues = parserPattern.tryToParse(workload);
         if (parsedValues != null) {
            return parsedValues;
         }
      }
      return null;
   }

   protected final boolean isValid(int[] values) {
      if (values == null) {
         return false;
      }
      for (int v : values) {
         if (v == NOT_VALID) {
            return false;
         }
      }
      return true;
   }

   private static class ParserPattern {
      private final Pattern pattern;
      private final int[] positions;

      public ParserPattern(String pattern, int[] positions) {
         this.pattern = Pattern.compile(pattern);
         this.positions = positions;
      }

      public final int[] tryToParse(String workload) {
         Matcher matcher = pattern.matcher(workload);
         if (matcher.matches()) {
            int[] upperAndLowerBound = new int[positions.length];
            for (int i = 0; i < positions.length; ++i) {
               upperAndLowerBound[i] = parseInt(matcher.group(i));
            }
            return upperAndLowerBound;
         }
         return null;
      }

      private int parseInt(String number) {
         try {
            int val =  Integer.parseInt(number);
            return val <= 0 ? NOT_VALID : val;
         } catch (Exception e) {
            return NOT_VALID;
         }
      }
   }
}
