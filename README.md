# Processamento Massivo Reativo com Kafka, Paralelismo e Backpressure

Este projeto implementa um sistema de processamento massivo de arquivos utilizando arquitetura reativa com Quarkus (Mutiny), Kafka e MongoDB. Combinando **paralelismo controlado** e **backpressure**, o sistema processa milhões de registros com alta performance e baixo uso de memória.

---

## 🔍 Visão Geral

- Leitura de arquivos com stream e controle de memória
- Produção paralela de mensagens para Kafka
- Consumo escalável com backpressure
- Escrita eficiente no MongoDB
- Observabilidade com Prometheus e Jaeger

---

## 🚀 Tecnologias Utilizadas

- [Quarkus Reactive](https://quarkus.io/)
- [Mutiny](https://smallrye.io/smallrye-mutiny/)
- [Kafka](https://kafka.apache.org/)
- [MongoDB](https://www.mongodb.com/)
- [Prometheus](https://prometheus.io/)
- [Jaeger](https://www.jaegertracing.io/)

---

## ⚙️ Execução

### Pré-requisitos
- Java 21+
- Kafka + MongoDB em execução
- Maven 3.9+
- Docker (para observabilidade)

### Rodando a aplicação

```bash
./mvnw compile quarkus:dev
