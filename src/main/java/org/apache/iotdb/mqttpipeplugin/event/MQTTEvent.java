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

package org.apache.iotdb.mqttpipeplugin.event;

import org.apache.iotdb.pipe.api.event.Event;

import java.util.List;

public class MQTTEvent implements Event {

  private String device;

  private long timestamp;

  private List<String> measurements;

  private List<String> values;

  public MQTTEvent(String device, long timestamp, List<String> measurements, List<String> values) {
    this.device = device;
    this.timestamp = timestamp;
    this.measurements = measurements;
    this.values = values;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  @Override
  public String toString() {
    return "MQTTEvent{" +
            "device='" + device + '\'' +
            ", timestamp=" + timestamp +
            ", measurements=" + measurements +
            ", values=" + values +
            '}';
  }
}
