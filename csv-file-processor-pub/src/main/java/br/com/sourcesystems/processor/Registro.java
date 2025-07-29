package br.com.sourcesystems.processor;

import java.time.LocalDateTime;

import org.apache.commons.csv.CSVRecord;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record Registro(
    String nome,
    String email,
    String telefone,
    String cpf,
    LocalDateTime dataLeitura

) {
    public Registro(CSVRecord record) {
        this(
            record.get("nome"),
            record.get("email"),
            record.get("telefone"),
            record.get("cpf"),
            LocalDateTime.now()
        );
    }
    public Registro(String nome, String email, String telefone, String cpf) {
        this(nome, email, telefone, cpf, LocalDateTime.now());
    }
    
} 
