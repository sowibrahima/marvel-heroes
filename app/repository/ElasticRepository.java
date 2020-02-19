package repository;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        String query = "{" +
                "\"_source\": [\"id\", \"name\", \"imageUrl\", \"universe\", \"gender\"],\n" +
                "  \"from\": "+page+",\n" +
                "  \"size\": "+size+", \n" +
                "  \"query\": {\n" +
                "    \"multi_match\" : {\n" +
                "      \"query\":    \""+input+"\", \n" +
                "      \"fields\": [ \"name\", \"identity.aliases\", \"identity.secretIdentities\", \"description\", \"partners\"]\n" +
                "    }\n" +
                "  }" +
                "}";

        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(query))
                .thenApply(response -> {

                    List<SearchedHero> heroes = new ArrayList<>();
                    JsonNode rootNode = Json.parse(response.getBody());

                    for (JsonNode hitNode : rootNode.path("hits").path("hits")) {
                        JsonNode sourceNode = hitNode.path("_source");

                        heroes.add(new SearchedHero(sourceNode.get("id").asText(), sourceNode.get("imageUrl").asText(),
                                sourceNode.get("name").asText(), sourceNode.get("universe").asText(), sourceNode.get("gender").asText()));
                    }

                    int total = rootNode.path("total").path("value").asInt();
                    return (new PaginatedResults<SearchedHero>(total, page, (int) Math.ceil(1.*total/size), heroes));
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        String query = "{" +
                "\"_source\": [\"id\", \"name\", \"imageUrl\", \"universe\", \"gender\"],\n" +
                "  \"suggest\": {\n" +
                "      \"hero-suggest\" : {\n" +
                "          \"prefix\" : \""+input+"\", \n" +
                "          \"completion\" : { \n" +
                "              \"field\" : \"suggest\",\n" +
                "              \"skip_duplicates\": true,\n" +
                "              \"fuzzy\" : true\n" +
                "          }\n" +
                "      }\n" +
                "  }" +
                "}";

        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(query))
                .thenApply(response -> {
                    List<SearchedHero> heroes = new ArrayList<>();
                    JsonNode rootNode = Json.parse(response.getBody());
                    for (JsonNode optionNode : rootNode.path("suggest").path("hero-suggest").path("options")) {
                        JsonNode sourceNode = optionNode.path("_source");
                        heroes.add(new SearchedHero(sourceNode.get("id").asText(), sourceNode.get("imageUrl").asText(),
                                sourceNode.get("name").asText(), sourceNode.get("universe").asText(), sourceNode.get("gender").asText()));
                    }
                    return heroes;
                });
    }
}
