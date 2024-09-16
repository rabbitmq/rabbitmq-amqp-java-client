// Copyright (c) 2024 Broadcom. All Rights Reserved.
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
package com.rabbitmq.client.amqp.observation.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/** To generate documentation. */
public enum AmqpObservationDocumentation implements ObservationDocumentation {
  /** Observation for publishing a message. */
  PUBLISH_OBSERVATION {

    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultPublishObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
      return LowCardinalityTags.values();
    }
  },

  /** Observation for processing a message. */
  PROCESS_OBSERVATION {

    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultProcessObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
      return LowCardinalityTags.values();
    }
  };

  /** Low cardinality tags. */
  public enum LowCardinalityTags implements KeyName {

    /** A string identifying the messaging system. */
    MESSAGING_SYSTEM {

      @Override
      public String asString() {
        return "messaging.system";
      }
    },

    /** A string identifying the kind of messaging operation. */
    MESSAGING_OPERATION {

      @Override
      public String asString() {
        return "messaging.operation";
      }
    },

    /** A string identifying the protocol (AMQP). */
    NET_PROTOCOL_NAME {

      @Override
      public String asString() {
        return "net.protocol.name";
      }
    },

    /** A string identifying the protocol version (1.0). */
    NET_PROTOCOL_VERSION {

      @Override
      public String asString() {
        return "net.protocol.version";
      }
    },
  }

  /** High cardinality tags. */
  public enum HighCardinalityTags implements KeyName {
    MESSAGING_DESTINATION_NAME {

      @Override
      public String asString() {
        return "messaging.destination.name";
      }
    },

    MESSAGING_ROUTING_KEY {

      @Override
      public String asString() {
        return "messaging.rabbitmq.destination.routing_key";
      }
    },

    MESSAGING_SOURCE_NAME {

      @Override
      public String asString() {
        return "messaging.source.name";
      }
    },

    MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES {

      @Override
      public String asString() {
        return "messaging.message.payload_size_bytes";
      }
    },

    MESSAGING_MESSAGE_ID {

      @Override
      public String asString() {
        return "messaging.message.id";
      }
    },

    MESSAGING_MESSAGE_CONVERSATION_ID {

      @Override
      public String asString() {
        return "messaging.message.conversation_id";
      }
    },

    NET_SOCK_PEER_PORT {
      @Override
      public String asString() {
        return "net.sock.peer.port";
      }
    },

    NET_SOCK_PEER_ADDR {
      @Override
      public String asString() {
        return "net.sock.peer.addr";
      }
    }
  }
}
