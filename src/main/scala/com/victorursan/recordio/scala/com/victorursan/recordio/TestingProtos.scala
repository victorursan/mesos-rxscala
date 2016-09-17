/*
 *    Copyright (C) 2015 Mesosphere, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.victorursan.recordio.scala.com.victorursan.recordio

import org.apache.mesos.v1.Protos._
import org.apache.mesos.v1.scheduler.Protos.Event

import scala.collection.JavaConverters._

/**
  * A set of utilities that are useful when testing code that requires instances of Mesos Protos
  */
object TestingProtos {
  /**
    * A predefined instance of {@link org.apache.mesos.v1.scheduler.Event Event} representing a
    * {@link org.apache.mesos.v1.scheduler.Event.Type#HEARTBEAT HEARTBEAT} message.
    */
  val HEARTBEAT: Event = Event.newBuilder
    .setType(Event.Type.HEARTBEAT)
    .build
  /**
    * A predefined instance of {@link org.apache.mesos.v1.scheduler.Event Event} representing a
    * {@link org.apache.mesos.v1.scheduler.Event.Type#SUBSCRIBED SUBSCRIBED} message with the following values
    * set:
    * <ul>
    * <li>{@code frameworkId = "a7cfd25c-79bd-481c-91cc-692e5db1ec3d"}</li>
    * <li>{@code heartbeatIntervalSeconds = 15}</li>
    * </ul>
    */
  val SUBSCRIBED: Event = Event.newBuilder
    .setType(Event.Type.SUBSCRIBED)
    .setSubscribed(Event.Subscribed.newBuilder
                     .setFrameworkId(FrameworkID.newBuilder
                                       .setValue("a7cfd25c-79bd-481c-91cc-692e5db1ec3d"))
                     .setHeartbeatIntervalSeconds(15))
    .build
  /**
    * A predefined instance of {@link org.apache.mesos.v1.scheduler.Event Event} representing a
    * {@link org.apache.mesos.v1.scheduler.Event.Type#OFFERS OFFERS} message containing a single offer with the
    * following values set:
    * <ul>
    * <li>{@code hostname = "host1"}</li>
    * <li>{@code offerId = "offer1"}</li>
    * <li>{@code agentId = "agent1"}</li>
    * <li>{@code frameworkId = "frw1"}</li>
    * <li>{@code cpus = 8.0}</li>
    * <li>{@code mem = 8192}</li>
    * <li>{@code disk = 8192}</li>
    * </ul>
    */
  val OFFER: Event = Event.newBuilder
    .setType(Event.Type.OFFERS)
    .setOffers(Event
                 .Offers
                 .newBuilder
                 .addAllOffers(
                   Seq(Offer.newBuilder
                             .setHostname("host1")
                             .setId(OfferID.newBuilder.setValue("offer1"))
                             .setAgentId(AgentID.newBuilder.setValue("agent1"))
                             .setFrameworkId(FrameworkID.newBuilder.setValue("frw1"))
                             .addResources(
                               Resource.newBuilder
                                 .setName("cpus")
                                 .setRole("*")
                                 .setType(Value.Type.SCALAR)
                                 .setScalar(Value.Scalar.newBuilder
                                              .setValue(8d)))
                             .addResources(
                               Resource.newBuilder
                                 .setName("mem")
                                 .setRole("*")
                                 .setType(Value.Type.SCALAR)
                                 .setScalar(Value.Scalar.newBuilder
                                              .setValue(8192L)))
                             .addResources(
                               Resource.newBuilder
                                 .setName("disk")
                                 .setRole("*")
                                 .setType(Value.Type.SCALAR)
                                 .setScalar(Value.Scalar.newBuilder
                                              .setValue(8192L)))
                             .build).asJava)).build
}

