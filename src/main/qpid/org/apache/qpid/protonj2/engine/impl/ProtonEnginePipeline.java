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
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.EnginePipeline;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.ProtonException;

/**
 * Pipeline of {@link EngineHandler} instances used to process IO
 */
public class ProtonEnginePipeline implements EnginePipeline {

    EngineHandlerContextReadBoundary head;
    EngineHandlerContextWriteBoundary tail;

    private final ProtonEngine engine;

    ProtonEnginePipeline(ProtonEngine engine) {
        if (engine == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.engine = engine;

        head = new EngineHandlerContextReadBoundary();
        tail = new EngineHandlerContextWriteBoundary();

        // Ensure Pipeline starts out empty but initialized.
        head.next = tail;
        head.previous = head;

        tail.previous = head;
        tail.next = tail;
    }

    @Override
    public ProtonEngine engine() {
        return engine;
    }

    @Override
    public ProtonEnginePipeline addFirst(String name, EngineHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonEngineHandlerContext oldFirst = head.next;
        ProtonEngineHandlerContext newFirst = createContext(name, handler);

        newFirst.next = oldFirst;
        newFirst.previous = head;

        oldFirst.previous = newFirst;
        head.next = newFirst;

        try {
            newFirst.handler().handlerAdded(newFirst);
        } catch (Throwable e) {
            engine.engineFailed(e);
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline addLast(String name, EngineHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonEngineHandlerContext oldLast = tail.previous;
        ProtonEngineHandlerContext newLast = createContext(name, handler);

        newLast.next = tail;
        newLast.previous = oldLast;

        oldLast.next = newLast;
        tail.previous = newLast;

        try {
            newLast.handler().handlerAdded(newLast);
        } catch (Throwable e) {
            engine.engineFailed(e);
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline removeFirst() {
        if (head.next != tail) {
            ProtonEngineHandlerContext oldFirst = head.next;

            head.next = oldFirst.next;
            head.next.previous = head;

            try {
                oldFirst.handler().handlerRemoved(oldFirst);
            } catch (Throwable e) {
                engine.engineFailed(e);
            }
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline removeLast() {
        if (tail.previous != head) {
            ProtonEngineHandlerContext oldLast = tail.previous;

            tail.previous = oldLast.previous;
            tail.previous.next = tail;

            try {
                oldLast.handler().handlerRemoved(oldLast);
            } catch (Throwable e) {
                engine.engineFailed(e);
            }
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline remove(String name) {
        if (name != null && !name.isEmpty()) {
            ProtonEngineHandlerContext current = head.next;
            ProtonEngineHandlerContext removed = null;
            while (current != tail) {
                if (current.name().equals(name)) {
                    removed = current;

                    ProtonEngineHandlerContext newNext = current.next;

                    current.previous.next = newNext;
                    newNext.previous = current.previous;

                    break;
                }

                current = current.next;
            }

            if (removed != null) {
                try {
                    removed.handler().handlerRemoved(removed);
                } catch (Throwable e) {
                    engine.engineFailed(e);
                }
            }
        }

        return this;
    }

    @Override
    public EnginePipeline remove(EngineHandler handler) {
        if (handler != null) {
            ProtonEngineHandlerContext current = head.next;
            ProtonEngineHandlerContext removed = null;
            while (current != tail) {
                if (current.handler() == handler) {
                    removed = current;

                    ProtonEngineHandlerContext newNext = current.next;

                    current.previous.next = newNext;
                    newNext.previous = current.previous;

                    break;
                }

                current = current.next;
            }

            if (removed != null) {
                try {
                    removed.handler().handlerRemoved(removed);
                } catch (Throwable e) {
                    engine.engineFailed(e);
                }
            }
        }

        return this;
    }

    @Override
    public EngineHandler find(String name) {
        EngineHandler handler = null;

        if (name != null && !name.isEmpty()) {
            ProtonEngineHandlerContext current = head.next;
            while (current != tail) {
                if (current.name().equals(name)) {
                    handler = current.handler();
                    break;
                }

                current = current.next;
            }
        }

        return handler;
    }

    @Override
    public EngineHandler first() {
        return head.next == tail ? null : head.next.handler();
    }

    @Override
    public EngineHandler last() {
        return tail.previous == head ? null : tail.previous.handler();
    }

    @Override
    public EngineHandlerContext firstContext() {
        return head.next == tail ? null : head.next;
    }

    @Override
    public EngineHandlerContext lastContext() {
        return tail.previous == head ? null : tail.previous;
    }

    //----- Event injection methods

    @Override
    public ProtonEnginePipeline fireEngineStarting() {
        ProtonEngineHandlerContext current = head;
        while (current != tail) {
            if (engine.state().ordinal() < EngineState.SHUTTING_DOWN.ordinal()) {
                try {
                    current.fireEngineStarting();
                } catch (Throwable error) {
                    engine.engineFailed(error);
                    break;
                }
                current = current.next;
            }
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireEngineStateChanged() {
        try {
            head.fireEngineStateChanged();
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireFailed(EngineFailedException e) {
        try {
            head.fireFailed(e);
        } catch (Throwable error) {
            // Ignore errors from handlers as engine is already failed.
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(ProtonBuffer input) {
        try {
            tail.fireRead(input);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(HeaderEnvelope header) {
        try {
            tail.fireRead(header);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(SASLEnvelope envelope) {
        try {
            tail.fireRead(envelope);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(IncomingAMQPEnvelope envelope) {
        try {
            tail.fireRead(envelope);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(HeaderEnvelope envelope) {
        try {
            head.fireWrite(envelope);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(OutgoingAMQPEnvelope envelope) {
        try {
            head.fireWrite(envelope);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(SASLEnvelope envelope) {
        try {
            head.fireWrite(envelope);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(ProtonBuffer buffer, Runnable ioComplete) {
        try {
            head.fireWrite(buffer, ioComplete);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    //----- Internal implementation

    private ProtonEngineHandlerContext createContext(String name, EngineHandler handler) {
        return new ProtonEngineHandlerContext(name, engine, handler);
    }

    //----- Synthetic handler context that bounds the pipeline

    private class EngineHandlerContextReadBoundary extends ProtonEngineHandlerContext {

        public EngineHandlerContextReadBoundary() {
            super("Read Boundary", engine, new BoundaryEngineHandler());
        }

        @Override
        public void fireRead(ProtonBuffer buffer) {
            throw engine.engineFailed(new ProtonException("No handler processed Transport read event."));
        }

        @Override
        public void fireRead(HeaderEnvelope header) {
            throw engine.engineFailed(new ProtonException("No handler processed AMQP Header event."));
        }

        @Override
        public void fireRead(SASLEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed SASL performative event."));
        }

        @Override
        public void fireRead(IncomingAMQPEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed protocol performative event."));
        }
    }

    private class EngineHandlerContextWriteBoundary extends ProtonEngineHandlerContext {

        public EngineHandlerContextWriteBoundary() {
            super("Write Boundary", engine, new BoundaryEngineHandler());
        }

        @Override
        public void fireWrite(HeaderEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write AMQP Header event."));
        }

        @Override
        public void fireWrite(OutgoingAMQPEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write AMQP performative event."));
        }

        @Override
        public void fireWrite(SASLEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write SASL performative event."));
        }

        @Override
        public void fireWrite(ProtonBuffer buffer, Runnable ioComplete) {
            // When not handled in the handler chain the buffer write propagates to the
            // engine to be handed to any registered output handler.  The engine is then
            // responsible for error handling if nothing is registered there to handle the
            // output of frame data.
            try {
                engine.dispatchWriteToEventHandler(buffer, ioComplete);
            } catch (Throwable error) {
                throw engine.engineFailed(error);
            }
        }
    }

    //----- Default TransportHandler Used at the pipeline boundary

    private class BoundaryEngineHandler implements EngineHandler {

        @Override
        public void engineStarting(EngineHandlerContext context) {
        }

        @Override
        public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
            throw engine.engineFailed(new ProtonException("No handler processed Transport read event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, HeaderEnvelope header) {
            throw engine.engineFailed(new ProtonException("No handler processed AMQP Header event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, SASLEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed SASL performative read event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed protocol performative read event."));
        }

        @Override
        public void handleWrite(EngineHandlerContext context, HeaderEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write AMQP Header event."));
        }

        @Override
        public void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write AMQP performative event."));
        }

        @Override
        public void handleWrite(EngineHandlerContext context, SASLEnvelope envelope) {
            throw engine.engineFailed(new ProtonException("No handler processed write SASL performative event."));
        }
    }
}
