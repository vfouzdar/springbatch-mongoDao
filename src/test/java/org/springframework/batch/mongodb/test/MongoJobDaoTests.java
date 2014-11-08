package org.springframework.batch.mongodb.test;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
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