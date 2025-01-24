/*
 * Copyright (C) 2002-2024 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.rabbitmq.client.amqp.impl;


import java.io.Serializable;
import java.util.Comparator;

/**
 * A type-specific {@link Comparator}; provides methods to compare two primitive types both as
 * objects and as primitive types.
 *
 * <p>
 * Note that {@code fastutil} provides a corresponding abstract class that can be used to implement
 * this interface just by specifying the type-specific comparator.
 *
 * @see Comparator
 */
@FunctionalInterface
interface FastUtilIntComparator extends Serializable {
  /**
   * Compares its two primitive-type arguments for order. Returns a negative integer, zero, or a
   * positive integer as the first argument is less than, equal to, or greater than the second.
   *
   * @see java.util.Comparator
   * @return a negative integer, zero, or a positive integer as the first argument is less than, equal
   *         to, or greater than the second.
   */
  int compare(int k1, int k2);

}