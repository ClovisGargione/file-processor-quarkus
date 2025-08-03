package br.com.sourcesystems.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;



@ApplicationScoped
public class RegistroBatchSender {

    private static final Logger LOG = Logger.getLogger(RegistroBatchSender.class);

    @ConfigProperty(name = "producer.batch.size", defaultValue = "100")
    private int batchSize;

    @Inject
    @Channel("registros-csv") // canal configurado no application.properties
    private Emitter<List<Registro>> emitter;

    public Uni<Void> enviarLote(List<Registro> lote) {
        if (lote == null || lote.isEmpty()) {
        LOG.warn("Lote vazio, nada será enviado.");
        return Uni.createFrom().voidItem();
        }

        LOG.infof("Enviando lote com %d registros para o Kafka...", lote.size());

        try {
            if (emitter.isCancelled()) {
                LOG.warn("Emitter foi cancelado. Abortando envio de lote.");
                return Uni.createFrom().voidItem();
            }
            CompletionStage<Void> stage = emitter.send(lote);
            return Uni.createFrom().completionStage(stage)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                .invoke(() -> LOG.info("Lote enviado com sucesso"));
        } catch (Exception e) {
            LOG.error("Erro ao enviar lote para Kafka", e);
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Void> processarArquivo(Uni<List<Registro>> loteUni) {
        return loteUni.onItem().transformToMulti(lote -> {
            List<List<Registro>> batches = partition(lote, batchSize);
            return Multi.createFrom().iterable(batches);
        })
        .onItem().transformToUniAndConcatenate(this::enviarLote)
        .onFailure().invoke(t -> LOG.error("Erro durante o envio para Kafka", t))
        .collect().asList()  // coleta todos os Void (na prática, só sincroniza a conclusão)
        .replaceWithVoid(); 
    }

    public static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(new ArrayList<>(list.subList(i, Math.min(i + size, list.size()))));
        }
        return partitions;
    }
}
