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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
                "\"suggest\": {\n" +
                "        \"hero-suggest\" : {\n" +
                "            \"prefix\" : \""+input+"\", \n" +
                "            \"completion\" : { \n" +
                "                \"field\" : \"suggest\",\n" +
                "                \"skip_duplicates\": true,\n" +
                "                \"fuzzy\" : true\n" +
                "            }\n" +
                "        }\n" +
                "    }" +
                "}";

        return wsClient.url(elasticConfiguration.uri + "/_search")
                .post(Json.parse(query))
                 .thenApply(response -> {

                     ObjectMapper mapper = new ObjectMapper();
                     try {
                         JsonNode rootNode = mapper.readTree(response.getBody());
                         JsonNode suggestNode = rootNode.path("suggest");
                         drNode.
                     } catch (IOException e) {
                         e.printStackTrace();
                     }

                     /*

                     List<SearchedHero> heroes = null;
                     try {
                         heroes = Arrays.asList(mapper.readValue(response.getBody(), SearchedHero[].class));
                     } catch (IOException e) {
                         e.printStackTrace();
                         heroes = Collections.emptyList();
                     }*/
                     return CompletableFuture.completedFuture(new PaginatedResults<>(size, page, 1, heroes));
                 });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO
        // return wsClient.url(elasticConfiguration.uri + "...")
        //         .post(Json.parse("{ ... }"))
        //         .thenApply(response -> {
        //             return ...
        //         });
    }
}
