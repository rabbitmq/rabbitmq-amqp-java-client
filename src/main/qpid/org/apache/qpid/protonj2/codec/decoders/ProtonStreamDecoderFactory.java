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
package org.apache.qpid.protonj2.codec.decoders;

import org.apache.qpid.protonj2.codec.decoders.ProtonStreamDecoder.DecoderMode;
import org.apache.qpid.protonj2.codec.decoders.messaging.AcceptedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.AmqpSequenceTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.AmqpValueTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeleteOnCloseTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeleteOnNoLinksOrMessagesTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeleteOnNoLinksTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeleteOnNoMessagesTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.MessageAnnotationsTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.ModifiedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.ReceivedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.RejectedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.ReleasedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.SourceTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.TargetTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.security.SaslChallengeTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.security.SaslInitTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.security.SaslMechanismsTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.security.SaslOutcomeTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.security.SaslResponseTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.CoordinatorTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.DeclareTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.DeclaredTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.DischargeTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.TransactionStateTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.AttachTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.BeginTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.CloseTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.DetachTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.DispositionTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.EndTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.ErrorConditionTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.FlowTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.OpenTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transport.TransferTypeDecoder;

/**
 * Factory that create and initializes new BuiltinDecoder instances
 */
public final class ProtonStreamDecoderFactory {

    private ProtonStreamDecoderFactory() {
    }

    /**
     * @return a new {@link ProtonDecoder} instance that only decodes AMQP types.
     */
    public static ProtonStreamDecoder create() {
        ProtonStreamDecoder decoder = new ProtonStreamDecoder();

        addMessagingTypeDecoders(decoder);
        addTransactionTypeDecoders(decoder);
        addTransportTypeDecoders(decoder);

        return decoder;
    }

    /**
     * @return a new {@link ProtonDecoder} instance that only decodes SASL types.
     */
    public static ProtonStreamDecoder createSasl() {
        final ProtonStreamDecoder decoder = new ProtonStreamDecoder(DecoderMode.SASL);

        addSaslTypeDecoders(decoder);

        return decoder;
    }

    private static void addMessagingTypeDecoders(ProtonStreamDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(AcceptedTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(AmqpSequenceTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(AmqpValueTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(ApplicationPropertiesTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DataTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeleteOnCloseTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeleteOnNoLinksOrMessagesTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeleteOnNoLinksTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeleteOnNoMessagesTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeliveryAnnotationsTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(FooterTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(HeaderTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(MessageAnnotationsTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(ModifiedTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(PropertiesTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(ReceivedTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(RejectedTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(ReleasedTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(SourceTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(TargetTypeDecoder.INSTANCE);
    }

    private static void addTransactionTypeDecoders(ProtonStreamDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(CoordinatorTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeclaredTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DeclareTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DischargeTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(TransactionStateTypeDecoder.INSTANCE);
    }

    private static void addTransportTypeDecoders(ProtonStreamDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(AttachTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(BeginTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(CloseTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DetachTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(DispositionTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(EndTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(ErrorConditionTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(FlowTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(OpenTypeDecoder.INSTANCE);
        Decoder.registerDescribedTypeDecoder(TransferTypeDecoder.INSTANCE);
    }

    private static void addSaslTypeDecoders(ProtonStreamDecoder decoder) {
        decoder.registerDescribedTypeDecoder(SaslChallengeTypeDecoder.INSTANCE);
        decoder.registerDescribedTypeDecoder(SaslInitTypeDecoder.INSTANCE);
        decoder.registerDescribedTypeDecoder(SaslMechanismsTypeDecoder.INSTANCE);
        decoder.registerDescribedTypeDecoder(SaslOutcomeTypeDecoder.INSTANCE);
        decoder.registerDescribedTypeDecoder(SaslResponseTypeDecoder.INSTANCE);
    }
}
