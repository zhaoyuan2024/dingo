/*
 * Copyright 2021 DataCanvas
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

package io.dingodb.coordinator.store;

import java.util.concurrent.CompletableFuture;

public interface AsyncKeyValueStore {

    /**
     * Returns true if executorView contains the key.
     * @param key key
     * @return true if executorView contains the key, else false
     */
    CompletableFuture<Boolean> contains(byte[] key);

    /**
     * Save the value to executorView by key, if this executorView previously contained a value for the key,
     * replace the old value with the specified value.
     * @param key key
     * @param value value
     */
    CompletableFuture<Boolean> put(byte[] key, byte[] value);

    /**
     * Removes the value for key from this executorView if it is present.
     * @param key key
     */
    CompletableFuture<Boolean> delete(byte[] key);

    /**
     * Returns the value if this executorView contains specified key, else returns null.
     * @param key key
     * @return value or null
     */
    CompletableFuture<byte[]> get(byte[] key);

    CompletableFuture<byte[]> getAndPut(byte[] key, byte[] value);

    CompletableFuture<byte[]> merge(byte[] key, byte[] value);

    CompletableFuture<Long> increment(byte[] key);

    void watchKeyRead(byte[] key, WatchKeyHandler handler);

    void watchKeyWrite(byte[] key, WatchKeyHandler handler);

    void watchKeyDelete(byte[] key, WatchKeyHandler handler);

    void watchKeyTouch(byte[] key, WatchKeyHandler handler);

    interface WatchKeyHandler {
        default void onRead(byte[] key, byte[] value, String stack) {

        }

        default void onWrite(byte[] key, byte[] value, String stack) {

        }

        default void onTouch(byte[] key, String stack) {

        }

        default void onDelete(byte[] key, String stack) {

        }
    }

}