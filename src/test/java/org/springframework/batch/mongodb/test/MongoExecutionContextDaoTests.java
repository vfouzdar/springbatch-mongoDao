package org.springframework.batch.mongodb.test;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Test CRUD for ExecutionContext in Mongo DB
 * 
 * @author Baruch S.
 * @author vfouzdar
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
public class MongoExecutionContextDaoTests{
	
    @Autowired
    private JobExecutionDao jobExecutionDao;
    @Autowired
    private JobInstanceDao jobInstanceDao;
    @Autowired
    private StepExecutionDao stepExecutionDao;
    @Autowired
    private ExecutionContextDao executionContextDao;    

    @Autowired
    protected MongoTemplate mongoTemplate;

    private JobExecution jobExecution;
    
    private StepExecution stepExecution;
    
    @Before
    public void setUp() {
    	mongoTemplate.getDb().drop();
        JobInstance ji = jobInstanceDao.createJobInstance("testJob", new JobParameters());
        
        jobExecution = new JobExecution(ji,null);
        jobExecutionDao.saveJobExecution(jobExecution);
        stepExecution = new StepExecution("stepName", jobExecution);
        stepExecutionDao.saveStepExecution(stepExecution);
    }
    
    
    @Test
    public void testSaveAndFindJobContext() {

        ExecutionContext ctx = new ExecutionContext(Collections.<String, Object>singletonMap("key", "value"));
        jobExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(jobExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(jobExecution);
        assertEquals(ctx, retrieved);
    }

    
    @Test
    public void testSaveAndFindEmptyJobContext() {

        ExecutionContext ctx = new ExecutionContext();
        jobExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(jobExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(jobExecution);
        assertEquals(ctx, retrieved);
    }

    
    @Test
    public void testUpdateContext() {

        ExecutionContext ctx = new ExecutionContext(Collections
                .<String, Object>singletonMap("key", "value"));
        jobExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(jobExecution);

        ctx.putLong("longKey", 7);
        executionContextDao.updateExecutionContext(jobExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(jobExecution);
        assertEquals(ctx, retrieved);
        assertEquals(7, retrieved.getLong("longKey"));
    }

    
    @Test
    public void testSaveAndFindStepContext() {

        ExecutionContext ctx = new ExecutionContext(Collections.<String, Object>singletonMap("key", "value"));
        stepExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(stepExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
    }

    
    @Test
    public void testSaveAndFindEmptyStepContext() {

        ExecutionContext ctx = new ExecutionContext();
        stepExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(stepExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
    }

    
    @Test
    public void testUpdateStepContext() {

        ExecutionContext ctx = new ExecutionContext(Collections.<String, Object>singletonMap("key", "value"));
        stepExecution.setExecutionContext(ctx);
        executionContextDao.saveExecutionContext(stepExecution);

        ctx.putLong("longKey", 7);
        executionContextDao.updateExecutionContext(stepExecution);

        ExecutionContext retrieved = executionContextDao.getExecutionContext(stepExecution);
        assertEquals(ctx, retrieved);
        assertEquals(7, retrieved.getLong("longKey"));
    }

    
    @Test
    public void testStoreInteger() {

        ExecutionContext ec = new ExecutionContext();
        ec.put("intValue", 343232);
        stepExecution.setExecutionContext(ec);
        executionContextDao.saveExecutionContext(stepExecution);
        ExecutionContext restoredEc = executionContextDao.getExecutionContext(stepExecution);
        assertEquals(ec, restoredEc);
        assertEquals(343232, restoredEc.getInt("intValue"));        
    }

}
