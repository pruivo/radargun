package org.radargun.stressors;/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/03/13
 */
public class ContentionStringKeyGenerator extends StringKeyGenerator{

   private static final Log log  = LogFactory.getLog(ContentionStringKeyGenerator.class);

   @Override
   public Object generateKey(int threadIndex, int keyIndex) {
      return super.generateKey(CONTEND, keyIndex);    //To change body of overridden methods use File | Settings | File Templates.
   }

   @Override
   public Object generateKey(int nodeIndex, int threadIndex, int keyIndex) {
      log.trace("Key "+super.generateKey(CONTEND, CONTEND, keyIndex));
      return super.generateKey(CONTEND, CONTEND, keyIndex);    //To change body of overridden methods use File | Settings | File Templates.
   }
}
