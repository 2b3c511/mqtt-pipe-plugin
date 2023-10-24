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

package org.apache.iotdb.mqttpipeplugin.service;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.mqttpipeplugin.event.MQTTEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.type.Type;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MQTTPipeProcesser implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTPipeProcesser.class);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {

  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) throws Exception {

  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    eventProcess(tabletInsertionEvent, eventCollector);
  }

  private void eventProcess(
      TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector) {
    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        Tablet tablet = ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
        List<String> measurement = new ArrayList<>();
        List<String> values = new ArrayList<>();
        tablet.getSchemas().stream().forEach(t -> measurement.add(t.getMeasurementId()));
        tabletInsertionEvent.processRowByRow(
                (row, rowCollector) -> {
                  try {
                    for (int i = 0; i < measurement.size(); i++) {
                      values.add(String.valueOf(getValueBasedOnType(row, i)));
                    }
                    Event mqttEvent = new MQTTEvent(row.getDeviceId(), row.getTime(), measurement, values);
                    eventCollector.collect(mqttEvent);
                  } catch (Exception e) {
                    LOGGER.error("Error occurred while processing row.", e);
                  }
                });
      } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
        Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
        List<String> measurement = new ArrayList<>();
        List<String> values = new ArrayList<>();
        tablet.getSchemas().stream().forEach(t -> measurement.add(t.getMeasurementId()));
        tabletInsertionEvent.processRowByRow(
                (row, rowCollector) -> {
                  try {
                    for (int i = 0; i < measurement.size(); i++) {
                      values.add(String.valueOf(getValueBasedOnType(row, i)));
                    }
                    Event mqttEvent = new MQTTEvent(row.getDeviceId(), row.getTime(), measurement, values);
                    eventCollector.collect(mqttEvent);
                  } catch (Exception e) {
                    LOGGER.error("Error occurred while processing row.", e);
                  }
                });
      } else {
        LOGGER.warn(
                "IoTDBThriftSyncConnector only support "
                        + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
                        + "Ignore {}.",
                tabletInsertionEvent);
      }
    }catch (Exception e){
      LOGGER.error("eventProcess Error:",e);
    }
  }

  private double getValueBasedOnType(Row row, int i) throws IOException {
    Type type = row.getDataType(i);
    switch (type) {
      case INT32:
        return row.getInt(i);
      case INT64:
        return row.getLong(i);
      case FLOAT:
        return row.getFloat(i);
      case DOUBLE:
        return row.getDouble(i);
      case BOOLEAN:
        return row.getBoolean(i) ? 1 : 0;
      default:
        LOGGER.error("Unsupported data type: {}", type);
        return 0;
    }
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    tsFileInsertionEvent
        .toTabletInsertionEvents()
        .forEach(event -> eventProcess(event, eventCollector));
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {}
}
