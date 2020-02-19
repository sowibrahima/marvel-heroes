package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        logger.info("Increase hero in tops: " + statItem.name);
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        return redisConnection.async().zincrby("pageHits", 1, statItem.toJson().toString())
                .thenApply(res -> {
                    redisConnection.close(); return true;
                });
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        logger.info("Added hero as last visited: " + statItem.name);
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        return redisConnection.async().lpush("lastPageViewed", statItem.toJson().toString())
                .thenApply(res -> {
                    redisConnection.close(); return 1L;
                });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        return redisConnection.async().lrange("lastPageViewed", 0, count -1)
                .thenApply(res -> {
                    List<StatItem> pages = new ArrayList<>();
                    for (String page:res){
                        pages.add(StatItem.fromJson(page));
                    }
                    redisConnection.close(); return pages;
                });
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        return redisConnection.async().zrevrangeWithScores("pageHits", 0, count - 1)
                .thenApply(res -> {
                    List<TopStatItem> pages = new ArrayList<>();
                    for (ScoredValue<String> page:res){
                        pages.add(new TopStatItem(StatItem.fromJson(page.getValue()), (long) page.getScore()));
                    }
                    redisConnection.close(); return pages;
                });
    }
}
