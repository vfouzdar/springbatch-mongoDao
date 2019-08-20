package org.springframework.batch.mongodb.test.config;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.mongodb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchDaoConfig {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Bean
    public ExecutionContextDao executionContextDao(){
        MongoExecutionContextDao dao = new  MongoExecutionContextDao();
        dao.setMongoTemplate(mongoTemplate);
        return dao;
    }

    @Bean
    public JobExecutionDao jobExecutionDao(){
        MongoJobExecutionDao dao =  new MongoJobExecutionDao();
        dao.setMongoTemplate(mongoTemplate);
        return dao;
    }

    @Bean
    public JobInstanceDao jobInstanceDao(){
        MongoJobInstanceDao dao =  new MongoJobInstanceDao();
        dao.setMongoTemplate(mongoTemplate);
        return dao;
    }

    @Bean
    public StepExecutionDao  stepExecutionDao(){
        MongoStepExecutionDao dao =  new MongoStepExecutionDao();
        dao.setMongoTemplate(mongoTemplate);
        return dao;
    }

    @Bean
    public PlatformTransactionManager platformTransactionManager(MongoDbFactory mongoDbFactory){
        return new MongoTransactionManager(mongoDbFactory);
    }

    @Bean
    public MongoJobRepositoryFactoryBean jobRepository(PlatformTransactionManager platformTransactionManager){
        MongoJobRepositoryFactoryBean mongoJobRepositoryFactoryBean = new MongoJobRepositoryFactoryBean();
        mongoJobRepositoryFactoryBean.setExecutionContextDao(executionContextDao());
        mongoJobRepositoryFactoryBean.setJobExecutionDao(jobExecutionDao());
        mongoJobRepositoryFactoryBean.setJobInstanceDao(jobInstanceDao());
        mongoJobRepositoryFactoryBean.setStepExecutionDao(stepExecutionDao());
        mongoJobRepositoryFactoryBean.setTransactionManager(platformTransactionManager);
        return mongoJobRepositoryFactoryBean;
    }
}
