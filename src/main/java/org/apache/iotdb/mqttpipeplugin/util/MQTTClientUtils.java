/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.mqttpipeplugin.util;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTClientUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTClientUtils.class);

  public static void sendMQTTRequest(String device,String uri,String json) {
      BlockingConnection connection = null;
      try {
          MQTT mqtt = new MQTT();
          mqtt.setHost(uri);
          mqtt.setVersion("3.1.1");
          mqtt.setConnectAttemptsMax(30);
          mqtt.setReconnectDelay(10);
          connection = mqtt.blockingConnection();
          connection.connect();
          connection.publish(device.replace(".","/"), json.getBytes(), QoS.AT_LEAST_ONCE, false);
      }catch (Exception e) {
          LOGGER.error("sendMQTTRequest ERROR:",e);
      }finally {
          try {
              connection.disconnect();
          } catch (Exception e) {
              LOGGER.error("sendMQTTRequest connection close fail:",e);
          }
      }
  }
}
