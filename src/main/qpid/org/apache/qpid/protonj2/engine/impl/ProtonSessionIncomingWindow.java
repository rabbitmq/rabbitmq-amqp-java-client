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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.util.SequenceNumber;
import org.apache.qpid.protonj2.engine.util.UnsettledMap;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Tracks the incoming window and provides management of that window in relation to receiver links.
 * <p>
 * The incoming window decreases as {@link Transfer} frames arrive and is replenished when the user reads the
 * bytes received in the accumulated payload of a delivery.  The window is expanded by sending a {@link Flow}
 * frame to the remote with an updated incoming window value at configured intervals based on reads from the
 * pending deliveries.
 */
public class ProtonSessionIncomingWindow {

    private static final int DEFAULT_WINDOW_SIZE = Integer.MAX_VALUE; // biggest legal value

    private final ProtonSession session;
    private final ProtonEngine engine;

    // User configured incoming capacity for the session used to compute the incoming window
    private int incomingCapacity = 0;

    // Computed incoming window based on the incoming capacity minus bytes not yet read from deliveries.
    private int incomingWindow = 0;

    // Tracks the next expected incoming transfer ID from the remote
    private int nextIncomingId = 0;

    // Tracks the most recent delivery Id for validation against the next incoming delivery
    private SequenceNumber lastDeliveryid;

    private int maxFrameSize;
    private int incomingBytes;

    private UnsettledMap<ProtonIncomingDelivery> unsettled =
        new UnsettledMap<>(ProtonIncomingDelivery::getDeliveryIdInt);

    public ProtonSessionIncomingWindow(ProtonSession session) {
        this.session = session;
        this.engine = session.getConnection().getEngine();
        this.maxFrameSize = (int) session.getConnection().getMaxFrameSize();
    }

    public void setIncomingCapacity(int incomingCapacity) {
        this.incomingCapacity = incomingCapacity;
    }

    public int getIncomingCapacity() {
        return incomingCapacity;
    }

    public int getRemainingIncomingCapacity() {
        if (incomingCapacity <= 0 || maxFrameSize == UnsignedInteger.MAX_VALUE.intValue()) {
            return DEFAULT_WINDOW_SIZE;
        } else {
            return incomingCapacity - incomingBytes;
        }
    }

    /**
     * Initialize the session level window values on the outbound Begin
     *
     * @param begin
     *      The {@link Begin} performative that is about to be sent.
     *
     * @return the configured performative
     */
    Begin configureOutbound(Begin begin) {
        // Update as it might have changed if session created before connection open() called.
        this.maxFrameSize = (int) session.getConnection().getMaxFrameSize();

        return begin.setIncomingWindow(updateIncomingWindow());
    }

    /**
     * Update the session level window values based on remote information.
     *
     * @param begin
     *      The {@link Begin} performative received from the remote.
     *
     * @return the given performative for chaining
     */
    Begin handleBegin(Begin begin) {
        if (begin.hasNextOutgoingId()) {
            this.nextIncomingId = UnsignedInteger.valueOf(begin.getNextOutgoingId()).intValue();
        }

        return begin;
    }

    /**
     * Update the session window state based on an incoming {@link Flow} performative
     *
     * @param flow
     *      the incoming {@link Flow} performative to process.
     */
    Flow handleFlow(Flow flow) {
        return flow;
    }

    /**
     * Update the session window state based on an incoming {@link Transfer} performative
     *
     * @param transfer
     *      the incoming {@link Transfer} performative to process.
     * @param payload
     *      the payload that was transmitted with the incoming {@link Transfer}
     */
    Transfer handleTransfer(ProtonLink<?> link, Transfer transfer, ProtonBuffer payload) {
        incomingBytes += payload != null ? payload.getReadableBytes() : 0;
        incomingWindow--;
        nextIncomingId++;

        final ProtonIncomingDelivery delivery = link.remoteTransfer(transfer, payload);

        if (!delivery.isSettled() && !delivery.isRemotelySettled() && delivery.isFirstTransfer()) {
            unsettled.put((int) delivery.getDeliveryId(), delivery);
        }

        return transfer;
    }

    /**
     * Update the state of any received Transfers that are indicated in the disposition
     * with the state information conveyed therein.
     *
     * @param disposition
     *      The {@link Disposition} performative to process
     *
     * @return the {@link Disposition}
     */
    Disposition handleDisposition(Disposition disposition) {
        final int first = (int) disposition.getFirst();

        if (disposition.hasLast() && disposition.getLast() != first) {
            handleRangedDisposition(unsettled, disposition);
        } else {
            final ProtonIncomingDelivery delivery = disposition.getSettled() ?
                unsettled.remove(first) : unsettled.get(first);

            if (delivery != null) {
                delivery.getLink().remoteDisposition(disposition, delivery);
            }
        }

        return disposition;
    }

    private static void handleRangedDisposition(UnsettledMap<ProtonIncomingDelivery> unsettled, Disposition disposition) {
        // Dispositions cover a contiguous range in the map and since the tracker always moves forward
        // when appending new deliveries the range can wrap without needing a second iteration.
        if (disposition.getSettled()) {
            unsettled.removeEach((int) disposition.getFirst(), (int) disposition.getLast(), (delivery) -> {
                delivery.getLink().remoteDisposition(disposition, delivery);
            });
        } else {
            unsettled.forEach((int) disposition.getFirst(), (int) disposition.getLast(), (delivery) -> {
                delivery.getLink().remoteDisposition(disposition, delivery);
            });
        }
    }

    long updateIncomingWindow() {
        if (incomingCapacity <= 0 || maxFrameSize == UnsignedInteger.MAX_VALUE.longValue()) {
            incomingWindow = DEFAULT_WINDOW_SIZE;
        } else {
            incomingWindow = Integer.divideUnsigned(incomingCapacity - incomingBytes, maxFrameSize);
        }

        return incomingWindow;
    }

    public void writeFlow(ProtonReceiver link) {
        updateIncomingWindow();
        session.writeFlow(link);
    }

    //----- Access to internal state useful for tests

    public long getIncomingBytes() {
        return Integer.toUnsignedLong(incomingBytes);
    }

    public int getNextIncomingId() {
        return nextIncomingId;
    }

    public int getIncomingWindow() {
        return incomingWindow;
    }

    //----- Handle sender link actions in the session window context

    private final Disposition cachedDisposition = new Disposition();

    void processDisposition(ProtonReceiver receiver, ProtonIncomingDelivery delivery) {
        if (!delivery.isRemotelySettled()) {
            // Would only be tracked if not already remotely settled.
            if (delivery.isSettled()) {
                unsettled.remove((int) delivery.getDeliveryId());
            }

            cachedDisposition.reset();
            cachedDisposition.setFirst(delivery.getDeliveryId());
            cachedDisposition.setRole(Role.RECEIVER);
            cachedDisposition.setSettled(delivery.isSettled());
            cachedDisposition.setState(delivery.getState());

            engine.fireWrite(cachedDisposition, session.getLocalChannel());
        }
    }

    void processDisposition(DeliveryState state, long [] range) {
        unsettled.removeEach((int) range[0], (int) range[1], d -> { });
        cachedDisposition.reset();
        cachedDisposition.setFirst(range[0]);
        cachedDisposition.setLast(range[1]);
        cachedDisposition.setRole(Role.RECEIVER);
        cachedDisposition.setSettled(true);
        cachedDisposition.setState(state);

        engine.fireWrite(cachedDisposition, session.getLocalChannel());
    }

    void deliveryRead(ProtonIncomingDelivery delivery, int bytesRead) {
        this.incomingBytes -= bytesRead;
        if (incomingWindow == 0) {
            writeFlow(delivery.getLink());
        }
    }

    void validateNextDeliveryId(long deliveryId) {
        if (lastDeliveryid == null) {
            lastDeliveryid = new SequenceNumber((int) deliveryId);
        } else {
            if (lastDeliveryid.increment().compareTo((int) deliveryId) != 0) {
                session.getConnection().getEngine().engineFailed(
                    new ProtocolViolationException("Expected delivery-id " + lastDeliveryid + ", got " + deliveryId));
            }
        }
    }
}
