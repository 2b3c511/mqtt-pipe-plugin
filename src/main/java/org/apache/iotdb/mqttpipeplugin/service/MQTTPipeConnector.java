package org.apache.iotdb.mqttpipeplugin.service;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.annotation.JacksonAnnotation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.iotdb.mqttpipeplugin.event.MQTTEvent;
import org.apache.iotdb.mqttpipeplugin.util.MQTTClientUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTPipeConnector implements PipeConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQTTPipeConnector.class);

    private final String SEND_MQTT_URI = "connector.send_mqtt_uri";

    private String sendMqttUri = "";

    @Override
    public void validate(PipeParameterValidator validator) throws Exception {
        validator.validateRequiredAttribute(SEND_MQTT_URI);
    }

    @Override
    public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration pipeConnectorRuntimeConfiguration) throws Exception {
        sendMqttUri=parameters.getString(SEND_MQTT_URI);
        LOGGER.info("MQTTPipeConnector configuration: {}", parameters);
    }

    @Override
    public void handshake() throws Exception {

    }

    @Override
    public void heartbeat() throws Exception {

    }

    @Override
    public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    }

    @Override
    public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {

    }

    @Override
    public void transfer(Event event) {
        if (event instanceof MQTTEvent) {
            try {
                MQTTEvent mqttEvent = (MQTTEvent) event;
                String alarmEventStr = JSON.toJSONString(mqttEvent) ;
                MQTTClientUtils.sendMQTTRequest(mqttEvent.getDevice(),sendMqttUri, alarmEventStr);
            } catch (Exception e) {
                LOGGER.error("alarmPipeConnector transfer {} error,", sendMqttUri,e);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {

    }
}