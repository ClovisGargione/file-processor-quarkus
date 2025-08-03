package br.com.sourcesystems.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class FileIngestionService {

    private static final Logger LOG = Logger.getLogger(FileIngestionService.class);

    @ConfigProperty(name = "app.files.dir")
    private String filesDir;

    @ConfigProperty(name = "app.files.processed.dir")
    private String filesProcessedDir;

    private CsvFileProcessorBackPressure processor;

    private RegistroBatchSender sender;

    public FileIngestionService(CsvFileProcessorBackPressure processor, RegistroBatchSender sender) {
        this.processor = processor;
        this.sender = sender;
    }



    public Uni<Void> processAllFiles() {

        File dir = new File(filesDir);
        if (!dir.exists() || !dir.isDirectory()) {
            LOG.warn("Diretório de arquivos não encontrado: " + filesDir);
            return Uni.createFrom().voidItem();
        }

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null || files.length == 0) {
            return Uni.createFrom().voidItem();
        }

        return Multi.createFrom().items(files)
            .onItem().transformToUniAndConcatenate(file -> {
                Path origem = file.toPath();
                Path destino = Paths.get(filesProcessedDir, file.getName());

                Uni<Void> uniMove = Uni.createFrom().voidItem()
                    .invoke(() -> {
                        try {
                            Files.createDirectories(destino.getParent());
                            Files.move(origem, destino, StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            LOG.error("Erro ao mover arquivo", e);
                        }
                        LOG.info("Arquivo movido para: " + destino.toAbsolutePath());
                    });

                Multi<List<Registro>> loteUni = processor.processFile(destino.toFile());
                Uni<Void> processamentoCompleto = loteUni   
                                                        .onItem()
                                                        .transformToUniAndConcatenate(lote -> sender.processarArquivo(Uni.createFrom().item(lote))) // Multi<Void>
                                                        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                                                        .collect()
                                                        .asList()   // Uni<List<Void>>
                                                        .replaceWithVoid();

                return uniMove.chain(() -> processamentoCompleto);
            })
            .onFailure().invoke(t -> LOG.error("Erro no processamento de arquivos", t))
            .collect().asList()
            .replaceWithVoid();
    }

}