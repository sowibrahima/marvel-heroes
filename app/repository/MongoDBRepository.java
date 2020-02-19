package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import org.bson.types.ObjectId;
import play.libs.Json;
import utils.HeroSamples;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        String query = "{ id : \""+ heroId +"\"}";
        Document document = Document.parse(query);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
//        return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
         List<Document> pipeline = new ArrayList<>();
         Document sub_group = new Document();
         sub_group.put("_id","$yearAppearance");
         sub_group.put("count",new Document("$sum",1));

         Document group = new Document("$group",sub_group);
         Document sort = new Document("$sort",new Document("_id",1));

         pipeline.add(group);
         pipeline.add(sort);

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {

        //return CompletableFuture.completedFuture(new ArrayList<>());

        List<Document> pipeline = new ArrayList<>();
        Document match = new Document("$match", new Document("$powers", new Document("$exists", true).append("$ne", "")));
        Document unwind = new Document("$unwind", "$powers");
        Document group = new Document("$group", new Document("_id", new Document("power", "$powers")).append("count", new Document("$sum", 1)));
        Document sort = new Document("$sort", new Document("count", -1));
        Document limit = new Document("$limit", top);

        pipeline.add(match);
        pipeline.add(unwind);
        pipeline.add(group);
        pipeline.add(sort);
        pipeline.add(limit);
        
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").findPath("power").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
        // List<Document> pipeline = new ArrayList<>();
        // return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
        //         .thenApply(documents -> {
        //             return documents.stream()
        //                     .map(Document::toJson)
        //                     .map(Json::parse)
        //                     .map(jsonNode -> {
        //                         return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
        //                     })
        //                     .collect(Collectors.toList());
        //         });
    }

}
