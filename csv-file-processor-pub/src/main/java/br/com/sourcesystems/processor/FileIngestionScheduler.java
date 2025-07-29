package br.com.sourcesystems.processor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class FileIngestionScheduler {

    private static final Logger LOG = Logger.getLogger(FileIngestionScheduler.class);

    FileIngestionService ingestionService;

    private final AtomicBoolean rodando = new AtomicBoolean(false);

    public FileIngestionScheduler(FileIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Scheduled(every = "{ingestor.schedule.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public Uni<Void> executarLeituraArquivos() throws Exception {
        LOG.info("Iniciando execução do agendador de leitura de arquivos CSV");

        if (rodando.getAndSet(true)) {
            LOG.info("Job já em execução, ignorando essa execução");
            return Uni.createFrom().voidItem();
        }

        return ingestionService.processAllFiles()
            .onFailure().invoke(t -> LOG.error("Erro na execução da job", t))
            .eventually(() -> {
                rodando.set(false);
                return Uni.createFrom().voidItem();
            });
    }
}
