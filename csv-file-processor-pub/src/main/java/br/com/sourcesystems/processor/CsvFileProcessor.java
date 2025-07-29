package br.com.sourcesystems.processor;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CsvFileProcessor {

    private static final Logger LOG = Logger.getLogger(CsvFileProcessor.class);

    @ConfigProperty(name = "producer.batch.size", defaultValue = "100")
    private int batchSize;

    public Multi<List<Registro>> processFile(File file) {
        LOG.infof("Iniciando leitura do arquivo: %s", file.getAbsolutePath());
        CSVFormat format = CSVFormat.Builder.create()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreHeaderCase(true)
                .setTrim(true)
                .setDelimiter(';')
                .get();

        return Multi.createFrom().emitter((MultiEmitter<? super List<Registro>> emitter) -> {
            try (Reader reader = new FileReader(file);
                CSVParser csvParser = CSVParser.parse(reader, format)) {

                List<Registro> batch = new ArrayList<>(batchSize);
                for (CSVRecord record : csvParser) {
                    Registro r = new Registro(record);
                    batch.add(r);

                    if (batch.size() >= batchSize) {
                        emitter.emit(new ArrayList<>(batch));
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    emitter.emit(new ArrayList<>(batch));
                }
                emitter.complete();
            } catch (IOException e) {
                emitter.fail(new RuntimeException("Erro ao ler o arquivo: " + file.getName(), e));
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }




}

