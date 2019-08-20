package org.springframework.batch.mongodb.test.config;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;



@Configuration
public class BatchConfig implements BatchConfigurer {
    @Autowired
    private ExecutionContextDao executionContextDao;
    @Autowired
    private JobExecutionDao jobExecutionDao;
    @Autowired
    private JobInstanceDao jobInstanceDao;
    @Autowired
    private StepExecutionDao stepExecutionDao;

    @Autowired
    private MongoDbFactory mongoDbFactory;

    @Override
    public JobRepository getJobRepository() {
        return new SimpleJobRepository(
                jobInstanceDao,
                jobExecutionDao,
                stepExecutionDao,
                executionContextDao
        );
    }
    @Override
    public PlatformTransactionManager getTransactionManager() {
        return new MongoTransactionManager(mongoDbFactory);
    }
    @Override
    public SimpleJobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
    @Override
    public JobExplorer getJobExplorer() {
        return new SimpleJobExplorer(
                jobInstanceDao,
                jobExecutionDao,
                stepExecutionDao,
                executionContextDao
        );
    }
}
