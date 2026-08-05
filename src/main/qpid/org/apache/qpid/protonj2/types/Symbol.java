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
package org.apache.qpid.protonj2.types;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;

/**
 * Class that represents an AMQP Symbol value.  The creation of a Symbol object
 * occurs during a lookup operation which cannot find an already stored version
 * of the string or byte buffer view of the Symbol's ASCII bytes.
 */
public final class Symbol implements Comparable<Symbol> {

    private static final Symbol EMPTY_SYMBOL = new Symbol();

    private static final int SYMBOL_CACHE_INITIAL_CAPACITY = 2048;
    private static final int MAX_CACHED_SYMBOLS = 8192;
    private static final int MAX_CACHED_SYMBOL_SIZE = 64;

    private static final int SASL_SYMBOL_CACHE_INITIAL_CAPACITY = 32;
    private static final int MAX_CACHED_SASL_SYMBOLS = 128;
    private static final int MAX_CACHED_SASL_SYMBOL_SIZE = 32;

    /**
     * Larger cache used for normal operations after the SASL exchange has completed.
     */
    private static final SymbolCache SYMBOL_CACHE =
        new SymbolCache(MAX_CACHED_SYMBOLS, MAX_CACHED_SYMBOL_SIZE, SYMBOL_CACHE_INITIAL_CAPACITY);

    /**
     * Smaller cache meant to house the small commonly used Symbols during the SASL exchange.
     */
    private static final SymbolCache SASL_SYMBOL_CACHE =
        new SymbolCache(MAX_CACHED_SASL_SYMBOLS, MAX_CACHED_SASL_SYMBOL_SIZE, SASL_SYMBOL_CACHE_INITIAL_CAPACITY);

    private String symbolString;
    private final ProtonBuffer underlying;
    private final int hashCode;
    private final SymbolCache symbolCache;

    private Symbol() {
        this.underlying = ProtonBufferAllocator.defaultAllocator().allocate(0).convertToReadOnly();
        this.hashCode = 31;
        this.symbolString = "";
        this.symbolCache = null;
    }

    private Symbol(ProtonBuffer underlying, SymbolCache cache) {
        this.underlying = underlying;
        this.hashCode = underlying.hashCode();
        this.symbolCache = cache;
    }

    /**
     * @return the number of bytes that comprise the Symbol value.
     */
    public int getLength() {
        return underlying.getReadableBytes();
    }

    /**
     * @return a read-only view of the {@link Symbol} as a buffer of ASCII bytes.
     */
    public ProtonBuffer toASCII() {
        return underlying.copy(true);
    }

    @Override
    public int compareTo(Symbol other) {
        return underlying.compareTo(other.underlying);
    }

    @Override
    public String toString() {
        if (symbolString == null && underlying.getReadableBytes() > 0) {
            symbolString = symbolCache.toString(this);
        }

        return symbolString;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof Symbol) {
            return underlying.equals(((Symbol) other).underlying);
        }

        return false;
    }

    /**
     * Writes the internal {@link Symbol} bytes to the provided {@link ProtonBuffer}.  This
     * is a raw ASCII encoding of the Symbol without and AMQP type encoding.
     *
     * @param target
     * 		The buffer where the Symbol bytes should be written to.
     */
    public void writeTo(ProtonBuffer target) {
        target.ensureWritable(underlying.getReadableBytes());
        underlying.copyInto(underlying.getReadOffset(), target, target.getWriteOffset(), underlying.getReadableBytes());
        target.advanceWriteOffset(underlying.getReadableBytes());
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link String}
     * name of the {@link Symbol}.
     *
     * @param symbolVal
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol}.
     *
     * @param symbolBytes
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(ProtonBuffer symbolBytes) {
        return getSymbol(symbolBytes, false);
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol}.
     *
     * @param symbolBuffer
     * 		The {@link ProtonBuffer} version of the {@link Symbol} value.
     * @param copyOnCreate
     * 		Should the provided buffer be copied during creation of a new {@link Symbol}.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(ProtonBuffer symbolBuffer, boolean copyOnCreate) {
        if (symbolBuffer == null) {
            return null;
        } else if (symbolBuffer.getReadableBytes() == 0) {
            return EMPTY_SYMBOL;
        } else {
            return SYMBOL_CACHE.getSymbol(symbolBuffer, copyOnCreate);
        }
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link String}
     * name of the {@link Symbol}.
     *
     * @param stringValue
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(String stringValue) {
        if (stringValue == null) {
            return null;
        } else if (stringValue.isEmpty()) {
            return EMPTY_SYMBOL;
        } else {
            return SYMBOL_CACHE.getSymbol(stringValue);
        }
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol} from the smaller SASL Symbol cache. The symbols returned
     * here are meant to only be those used in the SASL exchange during connection
     * establishment..
     *
     * @param symbolBytes
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSASLSymbol(ProtonBuffer symbolBytes) {
        return getSASLSymbol(symbolBytes, false);
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol} from the smaller SASL Symbol cache. The symbols returned
     * here are meant to only be those used in the SASL exchange during connection
     * establishment..
     *
     * @param symbolBuffer
     * 		The {@link ProtonBuffer} version of the {@link Symbol} value.
     * @param copyOnCreate
     * 		Should the provided buffer be copied during creation of a new {@link Symbol}.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSASLSymbol(ProtonBuffer symbolBuffer, boolean copyOnCreate) {
        if (symbolBuffer == null) {
            return null;
        } else if (symbolBuffer.getReadableBytes() == 0) {
            return EMPTY_SYMBOL;
        } else {
            return SASL_SYMBOL_CACHE.getSymbol(symbolBuffer, copyOnCreate);
        }
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link String}
     * name of the {@link Symbol} from the smaller SASL Symbol cache. The symbols returned
     * here are meant to only be those used in the SASL exchange during connection
     * establishment..
     *
     * @param stringValue
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSASLSymbol(String stringValue) {
        if (stringValue == null) {
            return null;
        } else if (stringValue.isEmpty()) {
            return EMPTY_SYMBOL;
        } else {
            return SASL_SYMBOL_CACHE.getSymbol(stringValue);
        }
    }

    private static class SymbolCache {

        private final Map<ProtonBuffer, Symbol> bufferToSymbols;
        private final Map<String, Symbol> stringToSymbols;

        private final int maxCachedSymbols;
        private final int maxCachedSymbolSize;

        public SymbolCache(int maxCachedSymbols, int maxCachedSymbolSize, int initialCapacity) {
            this.maxCachedSymbols = maxCachedSymbols;
            this.maxCachedSymbolSize = maxCachedSymbolSize;

            this.bufferToSymbols = new ConcurrentHashMap<>(initialCapacity);
            this.stringToSymbols = new ConcurrentHashMap<>(initialCapacity);
        }

        public String toString(Symbol symbol) {
            if (symbol.symbolString == null && symbol.getLength() > 0) {
                symbol.symbolString = symbol.underlying.toString(US_ASCII);

                if (symbol.getLength() <= maxCachedSymbolSize && stringToSymbols.size() < maxCachedSymbols) {
                    Symbol existing = null;

                    synchronized (this) {
                        if (stringToSymbols.size() < maxCachedSymbols) {
                            existing = stringToSymbols.putIfAbsent(symbol.symbolString, symbol);
                        }
                    }

                    if (existing != null) {
                        symbol.symbolString = existing.symbolString;
                    }
                }
            }

            return symbol.symbolString;
        }

        public Symbol getSymbol(String stringValue) {
            final boolean canCache = stringValue.length() <= maxCachedSymbolSize;

            Symbol symbol = null;

            if (canCache) {
                symbol = stringToSymbols.get(stringValue);
            }

            if (symbol == null) {
                symbol = getSymbol(ProtonBufferAllocator.defaultAllocator().copy(stringValue.getBytes(US_ASCII)), false);

                // For a new symbol instance we can give it a string value now and avoid any future need
                // to look in the cache for the mappings which saves some work later for Symbol::toString.
                if (symbol.symbolString == null) {
                    symbol.symbolString = stringValue;
                }

                // Don't cache overly large symbols to prevent holding large amount of memory in
                // the symbol cache and limit the cache to a cap to prevent over-large cache growth.
                if (canCache && stringToSymbols.size() < maxCachedSymbols) {
                    synchronized (this) {
                        if (stringToSymbols.size() < maxCachedSymbols) {
                            stringToSymbols.putIfAbsent(stringValue, symbol);
                        }
                    }
                }
            }

            return symbol;
        }

        public Symbol getSymbol(ProtonBuffer symbolBuffer, boolean copyOnCreate) {
            final boolean canCache = symbolBuffer.getReadableBytes() <= maxCachedSymbolSize;

            if (canCache) {
                Symbol symbol = bufferToSymbols.get(symbolBuffer);

                if (symbol == null) {
                    synchronized (this) {
                        symbol = bufferToSymbols.get(symbolBuffer);

                        if (symbol != null) {
                            return symbol;
                        }

                        symbol = createSymbol(this, symbolBuffer, copyOnCreate);

                        if (bufferToSymbols.size() < maxCachedSymbols) {
                            bufferToSymbols.put(symbol.underlying, symbol);
                        }
                    }
                }

                return symbol;
            } else {
                return createSymbol(this, symbolBuffer, copyOnCreate);
            }
        }
    }

    private static Symbol createSymbol(SymbolCache cache, ProtonBuffer symbolBuffer, boolean copyOnCreate) {
        if (copyOnCreate) {
            // Copy to a known heap based buffer to avoid issue with life-cycle of pooled buffer types.
            int symbolSize = symbolBuffer.getReadableBytes();
            ProtonBuffer copy = ProtonBufferAllocator.defaultAllocator().allocate(symbolSize);
            copy.writeBytes(symbolBuffer);
            symbolBuffer = copy.convertToReadOnly();
        }

        return new Symbol(symbolBuffer, cache);
    }
}
