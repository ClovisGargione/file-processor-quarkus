package br.com.sourcesystems.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CsvFileProcessorBackPressure {


    private static final Logger LOG = Logger.getLogger(CsvFileProcessorBackPressure.class);

    @ConfigProperty(name = "producer.batch.size", defaultValue = "100")
    private int batchSize;

    private static class CsvState {
        final Reader reader;
        final Iterator<CSVRecord> iterator;

        CsvState(Reader reader, Iterator<CSVRecord> iterator) {
            this.reader = reader;
            this.iterator = iterator;
        }
    }

    public Multi<List<Registro>> processFile(File file) {

        Multi<List<Registro>> multi = Multi.createFrom().generator(
        () -> {
                Reader reader = null;
                try {
                    reader = new FileReader(file);
                } catch (FileNotFoundException e) {
                    LOG.infof("Não foi possível ler o arquivo {}", file.getName());
                } 
                CSVFormat format = null;
                format = CSVFormat.Builder.create()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreHeaderCase(true)
                .setTrim(true)
                .setDelimiter(';')
                .get();
                    CSVParser csvParser = null;
                    try {
                        csvParser = CSVParser.parse(reader, format);
                    } catch (IOException e) {
                        LOG.error("");
                    }
                return new CsvState(reader, csvParser.iterator());
            },
            (state, emitter) -> {
                try {
                    List<Registro> batch = new ArrayList<>(batchSize);
                    while (state.iterator.hasNext() && batch.size() < batchSize) {
                        CSVRecord record = state.iterator.next();
                        batch.add(new Registro(record));
                    }

                    if (!batch.isEmpty()) {
                        emitter.emit(batch);
                    } else {
                        emitter.complete();
                        state.reader.close();
                    }
                } catch (Exception e) {
                    emitter.fail(e);
                }
                return state;
            }
        );

        return multi.runSubscriptionOn(Infrastructure.getDefaultWorkerPool());

    }

}
