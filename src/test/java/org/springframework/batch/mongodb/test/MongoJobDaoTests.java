package org.springframework.batch.mongodb.test;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.mongodb.test.config.BatchConfig;
import org.springframework.batch.mongodb.test.config.BatchDaoConfig;
import org.springframework.batch.mongodb.test.config.MongoConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MongoConfig.class, BatchDaoConfig.class, BatchConfig.class})
public class MongoJobDaoTests extends AbstractJobDaoTests{
	
	

	public static final String LONG_STRING = "A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String ";
	
	
	
	@Test
	public void testUpdateJobExecutionWithLongExitCode() {
		
		jobExecution.setExitStatus(ExitStatus.COMPLETED
				.addExitDescription(LONG_STRING));
		jobExecutionDao.updateJobExecution(jobExecution);

		JobExecution retirevedJE = jobExecutionDao.getJobExecution(jobExecution.getId());
				
		assertEquals(LONG_STRING, retirevedJE.getExitStatus().getExitDescription());
	}

}