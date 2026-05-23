package com.aiwarehouse.flink.feature_compute;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class UserEventDeserializer implements DeserializationSchema<OrderEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public OrderEvent deserialize(byte[] message) throws Exception {
        return MAPPER.readValue(message, OrderEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }
}
