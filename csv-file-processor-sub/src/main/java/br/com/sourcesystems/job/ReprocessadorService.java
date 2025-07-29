package br.com.sourcesystems.job;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.bson.Document;
import org.jboss.logging.Logger;


import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.quarkus.mongodb.reactive.ReactiveMongoCollection;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReprocessadorService {

    private static final Logger LOG = Logger.getLogger(ReprocessadorService.class);

    private static final String DATABASE_NAME = "arquivos";
    private static final String REGISTROS_COLLECTION = "registros";
    private static final String REGISTROS_FALHOS_COLLECTION = "registros_falhos";
    private static final String DOCUMENTOS_FIELD = "documento";
    private static final String ID_FIELD = "_id";
    private static final int LOTE = 100;
    private static final int PARALELISMO = 5; // disponível para uso futuro com flatMap

    private final ReactiveMongoClient mongoClient;

    public ReprocessadorService(ReactiveMongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Scheduled(every = "{schedule.interval}")
    public void reprocessarFalhas() {
        ReactiveMongoCollection<Document> falhosCollection = getCollection(REGISTROS_FALHOS_COLLECTION);

        Multi.createFrom().publisher(falhosCollection.find())
        .group().intoLists().of(LOTE)
        .onItem().transformToUni(this::processarLote)
        .merge(PARALELISMO) 
        .subscribe().with(
            ok -> LOG.info("Lotes reprocessados com sucesso"),
            err -> LOG.error("Erro ao reprocessar lotes", err)
        );
    }   

    private Uni<Void> processarLote(List<Document> lote) {
        if (lote.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        List<Document> documentosParaInserir = lote.stream()
            .flatMap(this::extrairDocumentos)
            .filter(Objects::nonNull)
            .toList();

        ReactiveMongoCollection<Document> registrosCollection = getCollection(REGISTROS_COLLECTION);
        ReactiveMongoCollection<Document> falhosCollection = getCollection(REGISTROS_FALHOS_COLLECTION);

        if (documentosParaInserir == null || documentosParaInserir.isEmpty()) {
            LOG.warn("Nenhum documento a inserir para o lote: " + lote);
            return Uni.createFrom().voidItem(); // ou Uni.createFrom().nullItem()
        }

        return registrosCollection.insertMany(documentosParaInserir)
            .onItem().transformToUni(v -> removerDocumentosFalhos(falhosCollection, lote))
            .onItem().invoke(() -> LOG.info("Lote processado e removido da fila de falhas"))
            .onFailure().invoke(err -> LOG.error("Erro ao processar lote", err))
            .replaceWithVoid();
    }

    private Uni<Void> removerDocumentosFalhos(ReactiveMongoCollection<Document> falhosCollection, List<Document> lote) {
        List<Document> filtros = lote.stream()
            .map(doc -> new Document(ID_FIELD, doc.getObjectId(ID_FIELD)))
            .toList();

        return falhosCollection.deleteMany(new Document("$or", filtros)).replaceWithVoid();
    }

    private Stream<Document> extrairDocumentos(Document doc) {
        List<Document> documentos = doc.getList(DOCUMENTOS_FIELD, Document.class);
        return documentos == null ? Stream.empty() : documentos.stream();
    }

    private ReactiveMongoCollection<Document> getCollection(String nome) {
        return mongoClient
            .getDatabase(DATABASE_NAME)
            .getCollection(nome, Document.class);
    }
}