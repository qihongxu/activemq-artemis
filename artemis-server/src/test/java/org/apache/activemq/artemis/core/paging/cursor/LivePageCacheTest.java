/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.paging.cursor;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.impl.LivePageCacheImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.junit.Assert;
import org.junit.Test;

public class LivePageCacheTest {

   private static final int MESSAGES = (32 * 3) + 1;

   private final LivePageCacheImpl cache;

   public LivePageCacheTest() throws Exception {
      final StorageManager storageManager = new NullStorageManager();
      final Page page = new Page(new SimpleString("something"), storageManager, null, null, 1);
      ICoreMessage msg = new CoreMessage().initBuffer(100);
      msg.setAddress("test");
      cache = new LivePageCacheImpl(page);
   }

   private static PagedMessageImpl createPagedMessage(long id) {
      ICoreMessage msg = new CoreMessage().initBuffer(100);
      msg.setMessageID(id);
      msg.setAddress("test");
      return new PagedMessageImpl(msg, null);
   }

   @Test
   public void shouldNumberOfMessagesBeTheSameOfTheAddedMessages() {
      final int messages = MESSAGES;
      for (int i = 0; i < messages; i++) {
         Assert.assertEquals(i, cache.getNumberOfMessages());
         cache.addLiveMessage(createPagedMessage(i));
      }
      Assert.assertEquals(messages, cache.getNumberOfMessages());
   }

   @Test
   public void shouldNumberOfMessagesBeTheSameOfTheAddedMessagesInBatch() {
      final int messages = MESSAGES;
      final PagedMessage[] pagedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         pagedMessages[i] = message;
      }
      cache.setMessages(pagedMessages);
      Assert.assertEquals(messages, cache.getNumberOfMessages());
   }

   @Test
   public void shouldGetMessageReturnNullIfEmpty() {
      Assert.assertNull(cache.getMessage(0));
   }

   @Test
   public void shouldGetMessageReturnNullIfExceedNumberOfMessages() {
      final int messages = MESSAGES;
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         cache.addLiveMessage(message);
         Assert.assertNull(cache.getMessage(i + 1));
      }
   }

   @Test
   public void shouldGetMessageReturnMessagesAccordingToAddLiveOrder() {
      final int messages = MESSAGES;
      final PagedMessage[] pagedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         pagedMessages[i] = message;
         cache.addLiveMessage(message);
      }
      final PagedMessage[] cachedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         cachedMessages[i] = cache.getMessage(i);
      }
      Assert.assertArrayEquals(cachedMessages, pagedMessages);
   }

   @Test
   public void shouldGetMessageReturnMessagesAccordingToSetMessagesOrder() {
      final int messages = MESSAGES;
      final PagedMessage[] pagedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         pagedMessages[i] = message;
      }
      cache.setMessages(pagedMessages);
      final PagedMessage[] cachedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         cachedMessages[i] = cache.getMessage(i);
      }
      Assert.assertArrayEquals(cachedMessages, pagedMessages);
   }

   @Test
   public void shouldGetMessagesReturnMessagesAccordingToAddLiveOrder() {
      final int messages = MESSAGES;
      final PagedMessage[] pagedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         pagedMessages[i] = message;
         cache.addLiveMessage(message);
      }
      final PagedMessage[] cachedMessages = cache.getMessages();
      Assert.assertArrayEquals(cachedMessages, pagedMessages);
   }

   @Test
   public void shouldGetMessagesReturnMessagesAccordingToSetMessagesOrder() {
      final int messages = MESSAGES;
      final PagedMessage[] pagedMessages = new PagedMessage[messages];
      for (int i = 0; i < messages; i++) {
         final PagedMessage message = createPagedMessage(i);
         pagedMessages[i] = message;
      }
      cache.setMessages(pagedMessages);
      final PagedMessage[] cachedMessages = cache.getMessages();
      Assert.assertArrayEquals(cachedMessages, pagedMessages);
   }

   @Test
   public void shouldGetMessagesReturnEmptyArrayIfEmpty() {
      Assert.assertArrayEquals(new PagedMessage[0], cache.getMessages());
   }

}
