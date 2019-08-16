package org.springframework.batch.mongodb.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.batch.core.JobExecution;


/**
 * Junits to test sequence CRUD 
 * @author vfouzdar
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
public class MongoDaoTests extends AbstractJobDaoTests{



    @Test
    public void testGetNextId() {
    	mongoTemplate.getDb().drop();
        for (long i = 1; i <= 100; i++) {        	
        	JobInstance ji = jobInstanceDao.createJobInstance("testJob_" + i, new JobParameters());
        	
        	JobExecution jobExecution = new JobExecution(ji,null);
        	
        	//Save JobExecution assigns next Sequence value to Id
            jobExecutionDao.saveJobExecution(jobExecution);
            
            //Last JobExecution for given JobInstance should have ID = counter
            JobExecution retrievedJE = jobExecutionDao.getLastJobExecution(ji);
            
            Assert.assertEquals(new Long(i), retrievedJE.getId());

        }
    }
}
