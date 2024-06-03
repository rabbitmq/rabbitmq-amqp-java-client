/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.types.transactions;

import org.apache.qpid.protonj2.types.Symbol;

public interface TxnCapability {

    final static Symbol LOCAL_TXN = Symbol.valueOf("amqp:local-transactions");

    final static Symbol DISTRIBUTED_TXN = Symbol.valueOf("amqp:distributed-transactions");

    final static Symbol PROMOTABLE_TXN = Symbol.valueOf("amqp:promotable-transactions");

    final static Symbol MULTI_TXNS_PER_SSN = Symbol.valueOf("amqp:multi-txns-per-ssn");

    final static Symbol MULTI_SSNS_PER_TXN = Symbol.valueOf("amqp:multi-ssns-per-txn");

}
