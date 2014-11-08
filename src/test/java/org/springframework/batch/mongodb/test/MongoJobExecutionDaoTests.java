package org.springframework.batch.mongodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
public class MongoJobExecutionDaoTests{

	@Autowired
	protected MongoTemplate mongoTemplate;
	
	@Autowired
    private JobExecutionDao jobExecutionDao;
    
	@Autowired
    private JobInstanceDao jobInstanceDao;
    
	
    protected JobInstance jobInstance;

    protected JobExecution execution;
    


    /**
     * @return tested object ready for use
     */
    protected StepExecutionDao getStepExecutionDao() {
        return null;
    }

    @Before
    public void onSetUp() throws Exception {
    	mongoTemplate.getDb().dropDatabase();
        jobInstance = jobInstanceDao.createJobInstance("Job1", new JobParameters());
        execution = new JobExecution(jobInstance, null);
    }

    /**
     * Save and find a job execution.
     */
    @Test
    public void testSaveAndFind() {

        execution.setStartTime(new Date(System.currentTimeMillis()));
        execution.setLastUpdated(new Date(System.currentTimeMillis()));
        execution.setExitStatus(ExitStatus.UNKNOWN);
        execution.setEndTime(new Date(System.currentTimeMillis()));
        jobExecutionDao.saveJobExecution(execution);

        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        assertEquals(1, executions.size());
        assertEquals(execution, executions.get(0));
        assertExecutionsAreEqual(execution, executions.get(0));
    }

    /**
     * Executions should be returned in the reverse order they were saved.
     */
    
    @Test
    public void testFindExecutionsOrdering() {

        List<JobExecution> execs = new ArrayList<JobExecution>();

        for (int i = 0; i < 10; i++) {
            JobExecution exec = new JobExecution(jobInstance, null);
            exec.setCreateTime(new Date(i));
            execs.add(exec);
            jobExecutionDao.saveJobExecution(exec);
        }

        List<JobExecution> retrieved = jobExecutionDao.findJobExecutions(jobInstance);
        Collections.reverse(retrieved);

        for (int i = 0; i < 10; i++) {
            assertExecutionsAreEqual(execs.get(i), retrieved.get(i));
        }

    }

    /**
     * Save and find a job execution.
     */
    
    @Test
    public void testFindNonExistentExecutions() {
        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        assertEquals(0, executions.size());
    }

    /**
     * Saving sets id to the entity.
     */
    
    @Test
    public void testSaveAddsIdAndVersion() {

        assertNull(execution.getId());
        assertNull(execution.getVersion());
        jobExecutionDao.saveJobExecution(execution);
        assertNotNull(execution.getId());
        assertNotNull(execution.getVersion());
    }

    /**
     * Update and retrieve job execution - check attributes have changed as
     * expected.
     */
    @Test
    public void testUpdateExecution() {
        execution.setStatus(BatchStatus.STARTED);
        jobExecutionDao.saveJobExecution(execution);

        execution.setLastUpdated(new Date(0));
        execution.setStatus(BatchStatus.COMPLETED);
        jobExecutionDao.updateJobExecution(execution);

        JobExecution updated = jobExecutionDao.findJobExecutions(jobInstance).get(0);
        assertEquals(execution, updated);
        assertEquals(BatchStatus.COMPLETED, updated.getStatus());
        assertExecutionsAreEqual(execution, updated);
    }

    /**
     * Check the execution with most recent start time is returned
     */
    @Test
    public void testGetLastExecution() {
        JobExecution exec1 = new JobExecution(jobInstance, null);
        exec1.setCreateTime(new Date(0));

        JobExecution exec2 = new JobExecution(jobInstance, null);
        exec2.setCreateTime(new Date(1));

        jobExecutionDao.saveJobExecution(exec1);
        jobExecutionDao.saveJobExecution(exec2);

        JobExecution last = jobExecutionDao.getLastJobExecution(jobInstance);
        assertEquals(exec2, last);
    }

    /**
     * Check the execution is returned
     */
    
    @Test
    public void testGetMissingLastExecution() {
        JobExecution value = jobExecutionDao.getLastJobExecution(jobInstance);
        assertNull(value);
    }

    /**
     * Check the execution is returned
     */
    @Test
    public void testFindRunningExecutions() {

        JobExecution exec = new JobExecution(jobInstance, null);
        exec.setCreateTime(new Date(0));
        exec.setEndTime(new Date(1L));
        exec.setLastUpdated(new Date(5L));
        jobExecutionDao.saveJobExecution(exec);

        exec = new JobExecution(jobInstance, null);
        exec.setLastUpdated(new Date(5L));
        exec.createStepExecution("step");
        jobExecutionDao.saveJobExecution(exec);

        StepExecutionDao stepExecutionDao = getStepExecutionDao();
        if (stepExecutionDao != null) {
            for (StepExecution stepExecution : exec.getStepExecutions()) {
                stepExecutionDao.saveStepExecution(stepExecution);
            }
        }

        Set<JobExecution> values = jobExecutionDao.findRunningJobExecutions(exec.getJobInstance().getJobName());

        assertEquals(1, values.size());
        JobExecution value = values.iterator().next();
        assertEquals(exec, value);
        assertEquals(5L, value.getLastUpdated().getTime());

    }

    /**
     * Check the execution is returned
     */
    @Test
    public void testNoRunningExecutions() {
        Set<JobExecution> values = jobExecutionDao.findRunningJobExecutions("no-such-job");
        assertEquals(0, values.size());
    }

    /**
     * Check the execution is returned
     */
    @Test
    public void testGetExecution() {
        JobExecution exec = new JobExecution(jobInstance, null);
        exec.setCreateTime(new Date(0));
        exec.createStepExecution("step");

        jobExecutionDao.saveJobExecution(exec);
        StepExecutionDao stepExecutionDao = getStepExecutionDao();
        if (stepExecutionDao != null) {
            for (StepExecution stepExecution : exec.getStepExecutions()) {
                stepExecutionDao.saveStepExecution(stepExecution);
            }
        }
        JobExecution value = jobExecutionDao.getJobExecution(exec.getId());

        assertEquals(exec, value);
        // N.B. the job instance is not re-hydrated in the JDBC case...
    }

    /**
     * Check the execution is returned
     */
    
    @Test
    public void testGetMissingExecution() {
        JobExecution value = jobExecutionDao.getJobExecution(54321L);
        assertNull(value);
    }

    /**
     * Exception should be raised when the version of update argument doesn't
     * match the version of persisted entity.
     */
    
    @Test(expected = OptimisticLockingFailureException.class)
    public void testConcurrentModificationException() {

        JobExecution exec1 = new JobExecution(jobInstance, null);
        jobExecutionDao.saveJobExecution(exec1);

        JobExecution exec2 = new JobExecution(jobInstance, null);
        exec2.setId(exec1.getId());

        exec2.incrementVersion();
        assertEquals((Integer) 0, exec1.getVersion());
        assertEquals(exec1.getVersion(), exec2.getVersion());

        jobExecutionDao.updateJobExecution(exec1);
        assertEquals((Integer) 1, exec1.getVersion());
        jobExecutionDao.updateJobExecution(exec2);
    }

    /**
     * Successful synchronization from STARTED to STOPPING status.
     */
    
    @Test
    public void testSynchronizeStatusUpgrade() {

        JobExecution exec1 = new JobExecution(jobInstance, null);
        exec1.setStatus(BatchStatus.STOPPING);
        jobExecutionDao.saveJobExecution(exec1);

        JobExecution exec2 = new JobExecution(jobInstance, null);
        Assert.state(exec1.getId() != null);
        exec2.setId(exec1.getId());

        exec2.setStatus(BatchStatus.STARTED);
        exec2.setVersion(7);
        Assert.state(!exec1.getVersion().equals(exec2.getVersion()));
        Assert.state(exec1.getStatus() != exec2.getStatus());

        jobExecutionDao.synchronizeStatus(exec2);

        assertEquals(exec1.getVersion(), exec2.getVersion());
        assertEquals(exec1.getStatus(), exec2.getStatus());
    }

    /**
     * UNKNOWN status won't be changed by synchronizeStatus, because it is the
     * 'largest' BatchStatus (will not downgrade).
     */    
    @Test
    public void testSynchronizeStatusDowngrade() {

        JobExecution exec1 = new JobExecution(jobInstance, null);
        exec1.setStatus(BatchStatus.STARTED);
        jobExecutionDao.saveJobExecution(exec1);

        JobExecution exec2 = new JobExecution(jobInstance, null);
        Assert.state(exec1.getId() != null);
        exec2.setId(exec1.getId());

        exec2.setStatus(BatchStatus.UNKNOWN);
        exec2.setVersion(7);
        Assert.state(!exec1.getVersion().equals(exec2.getVersion()));
        Assert.state(exec1.getStatus().isLessThan(exec2.getStatus()));

        jobExecutionDao.synchronizeStatus(exec2);

        assertEquals(exec1.getVersion(), exec2.getVersion());
        assertEquals(BatchStatus.UNKNOWN, exec2.getStatus());
    }

    /*
      * Check to make sure the executions are equal. Normally, comparing the id's
      * is sufficient. However, for testing purposes, especially of a DAO, we
      * need to make sure all the fields are being stored/retrieved correctly.
      */

    private void assertExecutionsAreEqual(JobExecution lhs, JobExecution rhs) {

        assertEquals(lhs.getId(), rhs.getId());
        assertEquals(lhs.getStartTime(), rhs.getStartTime());
        assertEquals(lhs.getStatus(), rhs.getStatus());
        assertEquals(lhs.getEndTime(), rhs.getEndTime());
        assertEquals(lhs.getCreateTime(), rhs.getCreateTime());
        assertEquals(lhs.getLastUpdated(), rhs.getLastUpdated());
        assertEquals(lhs.getVersion(), rhs.getVersion());
    }

}
