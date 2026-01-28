// Copyright (c) 2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.client.amqp.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link WorkPool} */
public class WorkPoolTest {

  private static final int QUEUE_LENGTH = 1000;

  private final WorkPool<String, Object> pool = new WorkPool<>();

  /** Test unknown key tolerated silently */
  @Test
  public void unknownKey() {
    assertFalse(this.pool.addWorkItem("test", new Object()));
  }

  /** Test add work and remove work */
  @Test
  public void basicInOut() {
    Object one = new Object();
    Object two = new Object();

    this.pool.registerKey("test", QUEUE_LENGTH);
    assertTrue(this.pool.addWorkItem("test", one));
    assertFalse(this.pool.addWorkItem("test", two));

    List<Object> workList = new ArrayList<Object>(16);
    String key = this.pool.nextWorkBlock(workList, 1);
    assertEquals("test", key);
    assertEquals(1, workList.size());
    assertEquals(one, workList.get(0));

    assertTrue(this.pool.finishWorkBlock(key), "Should be made ready");

    workList.clear();
    key = this.pool.nextWorkBlock(workList, 1);
    assertEquals("test", key, "Work client key wrong");
    assertEquals(two, workList.get(0), "Wrong work delivered");

    assertFalse(this.pool.finishWorkBlock(key), "Should not be made ready after this.");
    assertNull(this.pool.nextWorkBlock(workList, 1), "Shouldn't be more work");
  }

  /** Test add work when work in progress. */
  @Test
  public void workInWhileInProgress() {
    Object one = new Object();
    Object two = new Object();

    this.pool.registerKey("test", QUEUE_LENGTH);
    assertTrue(this.pool.addWorkItem("test", one));

    List<Object> workList = new ArrayList<Object>(16);
    String key = this.pool.nextWorkBlock(workList, 1);
    assertEquals("test", key);
    assertEquals(1, workList.size());
    assertEquals(one, workList.get(0));

    assertFalse(this.pool.addWorkItem("test", two));

    assertTrue(this.pool.finishWorkBlock(key));

    workList.clear();
    key = this.pool.nextWorkBlock(workList, 1);
    assertEquals("test", key);
    assertEquals(1, workList.size());
    assertEquals(two, workList.get(0));
  }

  /** Test multiple work keys. */
  @Test
  public void interleavingKeys() {
    Object one = new Object();
    Object two = new Object();
    Object three = new Object();

    this.pool.registerKey("test1", QUEUE_LENGTH);
    this.pool.registerKey("test2", QUEUE_LENGTH);

    assertTrue(this.pool.addWorkItem("test1", one));
    assertTrue(this.pool.addWorkItem("test2", two));
    assertFalse(this.pool.addWorkItem("test1", three));

    List<Object> workList = new ArrayList<Object>(16);
    String key = this.pool.nextWorkBlock(workList, 3);
    assertEquals("test1", key);
    assertEquals(2, workList.size());
    assertEquals(one, workList.get(0));
    assertEquals(three, workList.get(1));

    workList.clear();

    key = this.pool.nextWorkBlock(workList, 2);
    assertEquals("test2", key);
    assertEquals(1, workList.size());
    assertEquals(two, workList.get(0));
  }

  /** Test removal of key (with work) */
  @Test
  public void unregisterKey() {
    Object one = new Object();
    Object two = new Object();
    Object three = new Object();

    this.pool.registerKey("test1", QUEUE_LENGTH);
    this.pool.registerKey("test2", QUEUE_LENGTH);

    assertTrue(this.pool.addWorkItem("test1", one));
    assertTrue(this.pool.addWorkItem("test2", two));
    assertFalse(this.pool.addWorkItem("test1", three));

    this.pool.unregisterKey("test1");

    List<Object> workList = new ArrayList<Object>(16);
    String key = this.pool.nextWorkBlock(workList, 3);
    assertEquals("test2", key);
    assertEquals(1, workList.size());
    assertEquals(two, workList.get(0));
  }

  /** Test removal of all keys (with work). */
  @Test
  public void unregisterAllKeys() {
    Object one = new Object();
    Object two = new Object();
    Object three = new Object();

    this.pool.registerKey("test1", QUEUE_LENGTH);
    this.pool.registerKey("test2", QUEUE_LENGTH);

    assertTrue(this.pool.addWorkItem("test1", one));
    assertTrue(this.pool.addWorkItem("test2", two));
    assertFalse(this.pool.addWorkItem("test1", three));

    this.pool.unregisterAllKeys();

    List<Object> workList = new ArrayList<Object>(16);
    assertNull(this.pool.nextWorkBlock(workList, 1));
  }
}
