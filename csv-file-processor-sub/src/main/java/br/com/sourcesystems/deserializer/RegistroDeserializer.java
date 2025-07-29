package br.com.sourcesystems.deserializer;

import java.util.List;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import br.com.sourcesystems.consumer.Registro;

public class RegistroDeserializer implements Deserializer<List<Registro>> {

    private final ObjectMapper mapper;

    public RegistroDeserializer() {
        this.mapper = new ObjectMapper();
        mapper.registerModule(new ParameterNamesModule()); // 👈 ESSENCIAL para records
        this.mapper.registerModule(new JavaTimeModule());
        this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    @Override
    public List<Registro> deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            CollectionType type = mapper.getTypeFactory().constructCollectionType(List.class, Registro.class);
            List<Registro> result = mapper.readValue(data, type);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Erro ao desserializar o registro", e);
        }
    }

    @Override
    public void close() {
        // Não é necessário implementar nada aqui
    }

}
