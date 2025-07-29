package br.com.sourcesystems.serializer;

import java.util.List;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.com.sourcesystems.processor.Registro;

public class RegistroSerializer implements Serializer<List<Registro>> {

    private final ObjectMapper objectMapper;

    public RegistroSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(String topic, List<Registro> registros) {
        try {
            return objectMapper.writeValueAsBytes(registros);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao serializar registros", e);
        }
    }
}
