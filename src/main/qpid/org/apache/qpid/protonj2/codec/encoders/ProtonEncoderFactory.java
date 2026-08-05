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
package org.apache.qpid.protonj2.codec.encoders;

import org.apache.qpid.protonj2.codec.encoders.ProtonEncoder.EncoderMode;
import org.apache.qpid.protonj2.codec.encoders.messaging.AcceptedTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.AmqpSequenceTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.AmqpValueTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DataTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeleteOnCloseTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeleteOnNoLinksOrMessagesTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeleteOnNoLinksTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeleteOnNoMessagesTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.FooterTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.HeaderTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.MessageAnnotationsTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.ModifiedTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.ReceivedTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.RejectedTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.ReleasedTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.SourceTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.TargetTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.security.SaslChallengeTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.security.SaslInitTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.security.SaslMechanismsTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.security.SaslOutcomeTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.security.SaslResponseTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.CoordinatorTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.DeclareTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.DeclaredTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.DischargeTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.TransactionStateTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.AttachTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.BeginTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.CloseTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.DetachTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.DispositionTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.EndTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.ErrorConditionTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.FlowTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.OpenTypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.transport.TransferTypeEncoder;

/**
 * Factory that create and initializes new BuiltinEncoder instances
 */
public final class ProtonEncoderFactory {

    private ProtonEncoderFactory() {
    }

    /**
     * @return a new {@link ProtonEncoder} instance that only decodes AMQP types.
     */
    public static ProtonEncoder create() {
        final ProtonEncoder encoder = new ProtonEncoder();

        addMessagingTypeEncoders(encoder);
        addTransactionTypeEncoders(encoder);
        addTransportTypeEncoders(encoder);

        return encoder;
    }

    /**
     * @return a new {@link ProtonEncoder} instance that only decodes SASL types.
     */
    public static ProtonEncoder createSasl() {
        final ProtonEncoder encoder = new ProtonEncoder(EncoderMode.SASL);

        addSaslTypeEncoders(encoder);

        return encoder;
    }

    private static void addMessagingTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(AcceptedTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(AmqpSequenceTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(AmqpValueTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(ApplicationPropertiesTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DataTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeleteOnCloseTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeleteOnNoLinksOrMessagesTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeleteOnNoLinksTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeleteOnNoMessagesTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeliveryAnnotationsTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(FooterTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(HeaderTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(MessageAnnotationsTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(ModifiedTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(PropertiesTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(ReceivedTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(RejectedTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(ReleasedTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(SourceTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(TargetTypeEncoder.INSTANCE);
    }

    private static void addTransactionTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(CoordinatorTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeclaredTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DeclareTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DischargeTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(TransactionStateTypeEncoder.INSTANCE);
    }

    private static void addTransportTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(AttachTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(BeginTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(CloseTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DetachTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(DispositionTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(EndTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(ErrorConditionTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(FlowTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(OpenTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(TransferTypeEncoder.INSTANCE);
    }

    private static void addSaslTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(SaslChallengeTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(SaslInitTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(SaslMechanismsTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(SaslOutcomeTypeEncoder.INSTANCE);
        encoder.registerDescribedTypeEncoder(SaslResponseTypeEncoder.INSTANCE);
    }
}
