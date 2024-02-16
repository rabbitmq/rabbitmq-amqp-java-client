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
package com.rabbitmq.model.amqp;

import com.rabbitmq.model.Message;
import com.rabbitmq.model.ModelException;
import com.rabbitmq.model.Publisher;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

class AmqpPublisher implements Publisher {

  private final AmqpEnvironment environment;
  private final Sender sender;

  AmqpPublisher(AmqpEnvironment environment, String address) {
    this.environment = environment;
    try {
      this.sender =
          this.connection()
              .openSender(address, new SenderOptions().deliveryMode(DeliveryMode.AT_LEAST_ONCE));
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
    if (future.isDone()) return transformDoneFuture(future);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!future.isDone()) {
              awaitFutureIsDoneInForkJoinPool(future);
            }
            return future.get();
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new RuntimeException(e);
          }
        });
  }

  private static <T> CompletableFuture<T> transformDoneFuture(Future<T> future) {
    CompletableFuture<T> cf = new CompletableFuture<>();
    T result;
    try {
      result = future.get();
    } catch (Throwable ex) {
      cf.completeExceptionally(ex);
      return cf;
    }
    cf.complete(result);
    return cf;
  }

  private static void awaitFutureIsDoneInForkJoinPool(Future<?> future)
      throws InterruptedException {
    ForkJoinPool.managedBlock(
        new ForkJoinPool.ManagedBlocker() {
          @Override
          public boolean block() throws InterruptedException {
            try {
              future.get();
            } catch (ExecutionException e) {
              throw new RuntimeException(e);
            }
            return true;
          }

          @Override
          public boolean isReleasable() {
            return future.isDone();
          }
        });
  }

  private Connection connection() {
    return this.environment.connection();
  }

  @Override
  public Message message() {
    return new AmqpMessage();
  }

  @Override
  public void publish(Message message, ConfirmationHandler confirmationHandler) {
    try {
      Tracker tracker = this.sender.send(((AmqpMessage) message).nativeMessage());
      makeCompletableFuture(tracker.settlementFuture())
          .whenComplete(
              (t, throwable) -> {
                ConfirmationStatus status;
                if (throwable == null && t != null && t.remoteState() == DeliveryState.accepted()) {
                  status = ConfirmationStatus.CONFIRMED;
                } else {
                  status = ConfirmationStatus.FAILED;
                }
                confirmationHandler.handle(
                    new ConfirmationContext() {
                      @Override
                      public Message message() {
                        return message;
                      }

                      @Override
                      public ConfirmationStatus status() {
                        return status;
                      }
                    });
              });
    } catch (ClientException e) {
      throw new ModelException(e);
    }
  }

  @Override
  public void close() {
    this.sender.close();
  }
}
