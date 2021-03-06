/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPSecurityException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.runnables.AtomicRunnable;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

/**
 * This is the equivalent for the ServerProducer
 */
public class ProtonServerReceiverContext extends ProtonInitializable implements ProtonDeliveryHandler {

   private static final Logger log = Logger.getLogger(ProtonServerReceiverContext.class);

   protected final AMQPConnectionContext connection;

   protected final AMQPSessionContext protonSession;

   protected final Receiver receiver;

   protected SimpleString address;

   protected final AMQPSessionCallback sessionSPI;

   RoutingContext routingContext = new RoutingContextImpl(null);

   /**
    * We create this AtomicRunnable with setRan.
    * This is because we always reuse the same instance.
    * In case the creditRunnable was run, we reset and send it over.
    * We set it as ran as the first one should always go through
    */
   protected final AtomicRunnable creditRunnable;

   /**
    * This Credit Runnable may be used in Mock tests to simulate the credit semantic here
    */
   public static AtomicRunnable createCreditRunnable(int refill,
                                                     int threshold,
                                                     Receiver receiver,
                                                     AMQPConnectionContext connection) {
      Runnable creditRunnable = () -> {

         connection.requireInHandler();
         if (receiver.getCredit() <= threshold) {
            int topUp = refill - receiver.getCredit();
            if (topUp > 0) {
               // System.out.println("Sending " + topUp + " towards client");
               receiver.flow(topUp);
               connection.flush();
            }
         }
      };
      return new AtomicRunnable() {
         @Override
         public void atomicRun() {
            connection.runNow(creditRunnable);
         }
      };
   }

   /*
    The maximum number of credits we will allocate to clients.
    This number is also used by the broker when refresh client credits.
    */
   private final int amqpCredits;

   // Used by the broker to decide when to refresh clients credit.  This is not used when client requests credit.
   private final int minCreditRefresh;

   public ProtonServerReceiverContext(AMQPSessionCallback sessionSPI,
                                      AMQPConnectionContext connection,
                                      AMQPSessionContext protonSession,
                                      Receiver receiver) {
      this.connection = connection;
      this.protonSession = protonSession;
      this.receiver = receiver;
      this.sessionSPI = sessionSPI;
      this.amqpCredits = connection.getAmqpCredits();
      this.minCreditRefresh = connection.getAmqpLowCredits();
      this.creditRunnable = createCreditRunnable(amqpCredits, minCreditRefresh, receiver, connection).setRan();
   }

   @Override
   public void onFlow(int credits, boolean drain) {
      flow();
   }

   @Override
   public void initialise() throws Exception {
      super.initialise();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

      RoutingType defRoutingType;

      if (target != null) {
         if (target.getDynamic()) {
            // if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
            // will be deleted on closing of the session
            address = SimpleString.toSimpleString(sessionSPI.tempQueueName());
            defRoutingType = getRoutingType(target.getCapabilities(), address);

            try {
               sessionSPI.createTemporaryQueue(address, defRoutingType);
            } catch (ActiveMQAMQPSecurityException e) {
               throw e;
            } catch (ActiveMQSecurityException e) {
               throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingTempDestination(e.getMessage());
            } catch (Exception e) {
               throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
            }
            target.setAddress(address.toString());
         } else {
            // the target will have an address unless the remote is requesting an anonymous
            // relay in which case the address in the incoming message's to field will be
            // matched on receive of the message.
            address = SimpleString.toSimpleString(target.getAddress());

            if (address != null && !address.isEmpty()) {
               defRoutingType = getRoutingType(target.getCapabilities(), address);
               try {
                  if (!sessionSPI.checkAddressAndAutocreateIfPossible(address, defRoutingType)) {
                     throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
                  }
               } catch (ActiveMQAMQPNotFoundException e) {
                  throw e;
               } catch (Exception e) {
                  log.debug(e.getMessage(), e);
                  throw new ActiveMQAMQPInternalErrorException(e.getMessage(), e);
               }

               try {
                  sessionSPI.check(address, CheckType.SEND, new SecurityAuth() {
                     @Override
                     public String getUsername() {
                        String username = null;
                        SASLResult saslResult = connection.getSASLResult();
                        if (saslResult != null) {
                           username = saslResult.getUser();
                        }

                        return username;
                     }

                     @Override
                     public String getPassword() {
                        String password = null;
                        SASLResult saslResult = connection.getSASLResult();
                        if (saslResult != null) {
                           if (saslResult instanceof PlainSASLResult) {
                              password = ((PlainSASLResult) saslResult).getPassword();
                           }
                        }

                        return password;
                     }

                     @Override
                     public RemotingConnection getRemotingConnection() {
                        return connection.connectionCallback.getProtonConnectionDelegate();
                     }
                  });
               } catch (ActiveMQSecurityException e) {
                  throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingProducer(e.getMessage());
               }
            }
         }

         Symbol[] remoteDesiredCapabilities = receiver.getRemoteDesiredCapabilities();
         if (remoteDesiredCapabilities != null) {
            List<Symbol> list = Arrays.asList(remoteDesiredCapabilities);
            if (list.contains(AmqpSupport.DELAYED_DELIVERY)) {
               receiver.setOfferedCapabilities(new Symbol[]{AmqpSupport.DELAYED_DELIVERY});
            }
         }
      }
      flow();
   }

   public RoutingType getRoutingType(Receiver receiver, SimpleString address) {
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      return target != null ? getRoutingType(target.getCapabilities(), address) : getRoutingType((Symbol[]) null, address);
   }

   private RoutingType getRoutingType(Symbol[] symbols, SimpleString address) {
      if (symbols != null) {
         for (Symbol symbol : symbols) {
            if (AmqpSupport.TEMP_TOPIC_CAPABILITY.equals(symbol) || AmqpSupport.TOPIC_CAPABILITY.equals(symbol)) {
               return RoutingType.MULTICAST;
            } else if (AmqpSupport.TEMP_QUEUE_CAPABILITY.equals(symbol) || AmqpSupport.QUEUE_CAPABILITY.equals(symbol)) {
               return RoutingType.ANYCAST;
            }
         }
      }
      final AddressInfo addressInfo = sessionSPI.getAddress(address);
      if (addressInfo != null && !addressInfo.getRoutingTypes().isEmpty()) {
         if (addressInfo.getRoutingTypes().size() == 1 && addressInfo.getRoutingType() == RoutingType.MULTICAST) {
            return RoutingType.MULTICAST;
         }
      }
      RoutingType defaultRoutingType = sessionSPI.getDefaultRoutingType(address);
      defaultRoutingType = defaultRoutingType == null ? ActiveMQDefaultConfiguration.getDefaultRoutingType() : defaultRoutingType;
      return defaultRoutingType;
   }

   /*
    * called when Proton receives a message to be delivered via a Delivery.
    *
    * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
    */
   @Override
   public void onMessage(Delivery delivery) throws ActiveMQAMQPException {
      connection.requireInHandler();
      Receiver receiver = ((Receiver) delivery.getLink());

      if (receiver.current() != delivery) {
         return;
      }

      if (delivery.isAborted()) {
         // Aborting implicitly remotely settles, so advance
         // receiver to the next delivery and settle locally.
         receiver.advance();
         delivery.settle();

         // Replenish the credit if not doing a drain
         if (!receiver.getDrain()) {
            receiver.flow(1);
         }

         return;
      } else if (delivery.isPartial()) {
         return;
      }

      ReadableBuffer data = receiver.recv();
      receiver.advance();
      Transaction tx = null;

      if (delivery.getRemoteState() instanceof TransactionalState) {
         TransactionalState txState = (TransactionalState) delivery.getRemoteState();
         tx = this.sessionSPI.getTransaction(txState.getTxnId(), false);
      }

      final Transaction txUsed = tx;

      actualDelivery(delivery, receiver, data, txUsed);
   }

   private void actualDelivery(Delivery delivery, Receiver receiver, ReadableBuffer data, Transaction tx) {
      try {
         sessionSPI.serverSend(this, tx, receiver, delivery, address, delivery.getMessageFormat(), data, routingContext);
      } catch (Exception e) {
         log.warn(e.getMessage(), e);
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();

         if (e instanceof ActiveMQSecurityException) {
            condition.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
         } else {
            condition.setCondition(Symbol.valueOf("failed"));
         }
         connection.runLater(() -> {

            condition.setDescription(e.getMessage());
            rejected.setError(condition);

            delivery.disposition(rejected);
            delivery.settle();
            flow();
            connection.flush();
         });

      }
   }

   @Override
   public void close(boolean remoteLinkClose) throws ActiveMQAMQPException {
      protonSession.removeReceiver(receiver);
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      if (target != null && target.getDynamic() && (target.getExpiryPolicy() == TerminusExpiryPolicy.LINK_DETACH || target.getExpiryPolicy() == TerminusExpiryPolicy.SESSION_END)) {
         try {
            sessionSPI.removeTemporaryQueue(SimpleString.toSimpleString(target.getAddress()));
         } catch (Exception e) {
            //ignore on close, its temp anyway and will be removed later
         }
      }
   }

   @Override
   public void close(ErrorCondition condition) throws ActiveMQAMQPException {
      receiver.setCondition(condition);
      close(false);
   }

   public void flow() {
      connection.requireInHandler();
      if (!creditRunnable.isRun()) {
         return; // nothing to be done as the previous one did not run yet
      }

      creditRunnable.reset();

      // Use the SessionSPI to allocate producer credits, or default, always allocate credit.
      if (sessionSPI != null) {
         sessionSPI.flow(address, creditRunnable);
      } else {
         creditRunnable.run();
      }
   }

   public void drain(int credits) {
      connection.runNow(() -> {
         receiver.drain(credits);
         connection.flush();
      });
   }

   public int drained() {
      return receiver.drained();
   }

   public boolean isDraining() {
      return receiver.draining();
   }
}
