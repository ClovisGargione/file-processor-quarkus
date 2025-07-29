package br.com.sourcesystems.consumer;

import java.time.LocalDateTime;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record Registro(
    String nome,
    String email,
    String telefone,
    String cpf,
    LocalDateTime dataLeitura
) {      
} 
