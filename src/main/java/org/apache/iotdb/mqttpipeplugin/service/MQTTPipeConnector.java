package org.apache.iotdb.mqttpipeplugin.service;

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

    @Override
    public void validate(PipeParameterValidator validator) throws Exception {
    }

    @Override
    public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration pipeConnectorRuntimeConfiguration) throws Exception {

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
    }

    @Override
    public void close() throws Exception {

    }
}