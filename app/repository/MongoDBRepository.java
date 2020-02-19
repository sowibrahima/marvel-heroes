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
         List<Document> pipeline = new ArrayList<>();

         Document sub_match = new Document();
         sub_match.put("identity.yearAppearance",new Document("$ne",""));

         Document sub_group1 = new Document();
         sub_group1.put("_id",new Document("yearAppearance","$identity.yearAppearance").append("universe","$identity.universe"));
         sub_group1.put("count",new Document("$sum",1));
         Document sub_group2 = new Document();
         sub_group2.put("_id","$_id.yearAppearance");
         sub_group2.put("byUniverse",new Document("$push",new Document("universe","$_id.universe").append("count","$count")));

         Document match = new Document("$match",sub_match);
         Document group1 = new Document("$group",sub_group1);
         Document group2 = new Document("$group",sub_group2);

         pipeline.add(match);
         pipeline.add(group1);
         pipeline.add(group2);

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").asInt();
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

        List<Document> pipeline = new ArrayList<>();
        Document match = new Document("$match", new Document("powers", new Document("$exists", true).append("$ne", "")));
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
         List<Document> pipeline = new ArrayList<>();

         Document sub_group = new Document();
         sub_group.put("_id","$identity.universe");
         sub_group.put("count",new Document("$sum",1));

         Document group1 = new Document("$group",sub_group);
         pipeline.add(group1);

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

}
