package br.com.sourcesystems.consumer;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.bson.Document;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaConsumer {

    private static final Logger LOG = Logger.getLogger(KafkaConsumer.class);

    private final ReactiveMongoClient mongoClient;
    private Counter batchesSucesso;
    private Counter batchesFalha;

    private final MeterRegistry meterRegistry;

    private final ObjectMapper mapper;

    @ConfigProperty(name = "consumer.batch.size", defaultValue = "100")
    private int batchSize;

    @ConfigProperty(name = "consumer.batch.delay-ms", defaultValue = "50")
    private long delayMs;

    @ConfigProperty(name = "consumer.parallelism", defaultValue = "4")
    private int consumerParallelism;


    public KafkaConsumer(ReactiveMongoClient mongoClient, MeterRegistry meterRegistry) {
        this.mongoClient = mongoClient;
        this.meterRegistry = meterRegistry;
        this.mapper = new ObjectMapper();
    }

    @PostConstruct
    void initMetrics() {
        batchesSucesso = meterRegistry.counter("consumer.batches.sucesso");
        batchesFalha = meterRegistry.counter("consumer.batches.falha");
    }

    
    @Incoming("registros-csv")
    public CompletionStage<Void> receive(List<List<Registro>> registros) throws JsonMappingException, JsonProcessingException {
        //LOG.info("KafkaConsumer: recebendo registros");
        //List<Registro> registros = mapper.readValue(payload, new TypeReference<List<Registro>>() {});
        LOG.info("Total de registros: " + registros.size());
        LOG.info("KafkaConsumer: recebidos registros, total: " + (registros == null ? "null" : registros.size()));
        if (registros == null || registros.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<Registro> registrosUnicos = registros.stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
       
        
    return processarLote(registrosUnicos)
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onItem().invoke(() -> {
            LOG.info("Lote processado com sucesso");
            batchesSucesso.increment();
        })
        .onFailure().invoke(erro -> {
            LOG.error("Erro ao processar lote", erro);
            batchesFalha.increment();
            registros.forEach(r -> {
                r.forEach(s -> {
                    salvarErro(s, erro);
                });
            });
        })
        .subscribe().asCompletionStage(); 
    }

    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 10000)
    @Timed(value = "lote.tempo.processo", description = "Tempo para processar um lote")
    protected Uni<Void> processarLote(List<Registro> registros) {
        if (registros == null || registros.isEmpty()) {
            LOG.warn("Lote vazio recebido, ignorando");
            return Uni.createFrom().voidItem();
        }

        List<List<Registro>> subLotes = partitionList(registros, batchSize);
        LOG.info(registros.size() + " registros recebidos, iniciando processamento em sub-lotes de " + batchSize);
        return Multi.createFrom().iterable(subLotes)
            .onItem().transformToUni(subLote -> {
                if (subLote == null || subLote.isEmpty()) {
                    LOG.warn("Sub-lote vazio recebido, ignorando");
                    return Uni.createFrom().voidItem();
                }
                System.out.println("Tipo recebido: " + subLote.getClass());
                LOG.info("Processando sub-lote de " + subLote.size());
                List<Document> docs = subLote.stream()
                    .map(r -> new Document()
                        .append("nome", r.nome())
                        .append("email", r.email())
                        .append("telefone", r.telefone())
                        .append("cpf", r.cpf())
                        .append("dataLeitura", r.dataLeitura()))
                        .collect(Collectors.toList());
       
                return mongoClient.getDatabase("arquivos")
                    .getCollection("registros", Document.class)
                    .insertMany(docs)
                    .onItem().invoke(() -> batchesSucesso.increment())
                    .onFailure().invoke(e -> {
                        batchesFalha.increment();
                        subLote.forEach(r -> salvarErro(r, e));
                    })
                    .replaceWithVoid();
            }).merge(consumerParallelism)  // Espera o último elemento emitido (Uni<Void>)
            .collect().last().replaceWithVoid();
    }

    private List<List<Registro>> partitionList(List<Registro> list, int size) {
        List<List<Registro>> parts = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            parts.add(new ArrayList<>(list.subList(i, Math.min(i + size, list.size())))); // cria cópia
        }
        return parts;
    }

    private void salvarErro(Registro sub, Throwable erro) {
        LOG.error("Erro ao inserir sublote", erro);

        Document errorDoc = new Document()
            .append("erro", erro.getMessage())
            .append("documento", sub)
            .append("timestamp", System.currentTimeMillis());

        mongoClient.getDatabase("arquivos")
            .getCollection("registros_falhos", Document.class)
            .insertOne(errorDoc)
            .subscribe().with(
                success -> LOG.info("Sub-lote com erro armazenado para reprocessamento"),
                fail -> LOG.error("Erro ao salvar o sub-lote com erro", fail)
            );
    }

}