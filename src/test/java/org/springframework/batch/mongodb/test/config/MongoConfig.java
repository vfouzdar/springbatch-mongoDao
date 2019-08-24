package org.springframework.batch.mongodb.test.config;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.mongodb.test.MongoDbContainer;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
public class MongoConfig extends AbstractMongoConfiguration implements ApplicationContextAware {
    private static Logger logger = LoggerFactory.getLogger(MongoConfig.class);
    private ApplicationContext applicationContext;

    @Override
    protected String getDatabaseName() {
        return "test";
    }

    @Override
    @Bean
    public MongoClient mongoClient() {
        MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions.builder()
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .socketKeepAlive(true);
        MongoDbContainer mongo = mongoDbContainer();
        return new MongoClient(new MongoClientURI(String.format("mongodb://%s:%d/test", mongo.getContainerIpAddress(), mongo.getPort()), mongoClientOptionsBuilder));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    @Bean
    public MongoDbFactory mongoDbFactory() {
        return super.mongoDbFactory();
    }

    @Bean
    public MongoTemplate mongoTemplate(){
        return new MongoTemplate(mongoDbFactory());
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public MongoDbContainer mongoDbContainer(){
        MongoDbContainer mongo = new MongoDbContainer()
                .withCommand("--replSet rs");
        return mongo;
    }
}
