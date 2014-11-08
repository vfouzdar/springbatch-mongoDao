/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.mongodb.test;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.mongodb.test.support.StepSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Tests for {@link StepExecutionDao} implementations.
 *
 * @see #getStepExecutionDao()
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
public class MongoStepExecutionDaoTests {

    @Autowired
    private StepExecutionDao stepExecutionDao;

    @Autowired
    private JobRepository jobRepository;

    protected JobInstance jobInstance;

    protected JobExecution jobExecution;

    protected Step step;

    protected StepExecution stepExecution;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Before
    public void onSetUp() throws Exception {
        mongoTemplate.getDb().dropDatabase();
        jobExecution = jobRepository.createJobExecution("job", new JobParameters());
        jobInstance = jobExecution.getJobInstance();
        step = new StepSupport("foo");
        stepExecution = new StepExecution(step.getName(), jobExecution);
    }

    @Test
    public void testSaveExecutionAssignsIdAndVersion() throws Exception {

        assertNull(stepExecution.getId());
        assertNull(stepExecution.getVersion());
        stepExecutionDao.saveStepExecution(stepExecution);
        assertNotNull(stepExecution.getId());
        assertNotNull(stepExecution.getVersion());
    }

    @Test
    public void testSaveAndGetExecution() {

        stepExecution.setStatus(BatchStatus.STARTED);
        stepExecution.setReadSkipCount(7);
        stepExecution.setProcessSkipCount(2);
        stepExecution.setWriteSkipCount(5);
        stepExecution.setProcessSkipCount(11);
        stepExecution.setRollbackCount(3);
        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
        stepExecution.setReadCount(17);
        stepExecution.setFilterCount(15);
        stepExecution.setWriteCount(13);
        stepExecutionDao.saveStepExecution(stepExecution);

        StepExecution retrieved = stepExecutionDao.getStepExecution(jobExecution, stepExecution.getId());

        assertStepExecutionsAreEqual(stepExecution, retrieved);
        assertNotNull(retrieved.getVersion());
        assertNotNull(retrieved.getJobExecution());
        assertNotNull(retrieved.getJobExecution().getId());
        assertNotNull(retrieved.getJobExecution().getJobId());
        assertNotNull(retrieved.getJobExecution().getJobInstance());

    }

    @Test
    public void testSaveAndGetNonExistentExecution() {
        assertNull(stepExecutionDao.getStepExecution(jobExecution, 45677L));
    }

    @Test
    public void testSaveAndFindExecution() {

        stepExecution.setStatus(BatchStatus.STARTED);
        stepExecution.setReadSkipCount(7);
        stepExecution.setWriteSkipCount(5);
        stepExecution.setRollbackCount(3);
        stepExecutionDao.saveStepExecution(stepExecution);

        stepExecutionDao.addStepExecutions(jobExecution);
        Collection<StepExecution> retrieved = jobExecution.getStepExecutions();
        assertStepExecutionsAreEqual(stepExecution, retrieved.iterator().next());
    }

    @Test
    public void testGetForNotExistingJobExecution() {
        assertNull(stepExecutionDao.getStepExecution(new JobExecution(jobInstance, (long) 777, null, null), 11L));
    }

    /**
     * To-be-saved execution must not already have an id.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSaveExecutionWithIdAlreadySet() {
        stepExecution.setId((long) 7);
        stepExecutionDao.saveStepExecution(stepExecution);
    }

    /**
     * To-be-saved execution must not already have a version.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSaveExecutionWithVersionAlreadySet() {
        stepExecution.incrementVersion();
        stepExecutionDao.saveStepExecution(stepExecution);
    }

    /**
     * Update and retrieve updated StepExecution - make sure the update is
     * reflected as expected and version number has been incremented
     */
    @Test
    public void testUpdateExecution() {
        stepExecution.setStatus(BatchStatus.STARTED);
        stepExecutionDao.saveStepExecution(stepExecution);
        Integer versionAfterSave = stepExecution.getVersion();

        stepExecution.setStatus(BatchStatus.ABANDONED);
        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
        stepExecutionDao.updateStepExecution(stepExecution);
        assertEquals(versionAfterSave + 1, stepExecution.getVersion().intValue());

        StepExecution retrieved = stepExecutionDao.getStepExecution(jobExecution, stepExecution.getId());
        assertEquals(stepExecution, retrieved);
        assertEquals(stepExecution.getLastUpdated(), retrieved.getLastUpdated());
        assertEquals(BatchStatus.ABANDONED, retrieved.getStatus());
    }

    /**
     * Exception should be raised when the version of update argument doesn't
     * match the version of persisted entity.
     */
    @Test(expected = OptimisticLockingFailureException.class)
    public void testConcurrentModificationException() {
        step = new StepSupport("foo");

        StepExecution exec1 = new StepExecution(step.getName(), jobExecution);
        stepExecutionDao.saveStepExecution(exec1);

        StepExecution exec2 = new StepExecution(step.getName(), jobExecution);
        exec2.setId(exec1.getId());

        exec2.incrementVersion();
        assertEquals(new Integer(0), exec1.getVersion());
        assertEquals(exec1.getVersion(), exec2.getVersion());

        stepExecutionDao.updateStepExecution(exec1);
        assertEquals(new Integer(1), exec1.getVersion());
        stepExecutionDao.updateStepExecution(exec2);

    }

    @Test
    public void testGetStepExecutionsWhenNoneExist() throws Exception {
        int count = jobExecution.getStepExecutions().size();
        stepExecutionDao.addStepExecutions(jobExecution);
        assertEquals("Incorrect size of collection", count, jobExecution.getStepExecutions().size());
    }

    private void assertStepExecutionsAreEqual(StepExecution expected, StepExecution actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getEndTime(), actual.getEndTime());
        assertEquals(expected.getSkipCount(), actual.getSkipCount());
        assertEquals(expected.getCommitCount(), actual.getCommitCount());
        assertEquals(expected.getReadCount(), actual.getReadCount());
        assertEquals(expected.getWriteCount(), actual.getWriteCount());
        assertEquals(expected.getFilterCount(), actual.getFilterCount());
        assertEquals(expected.getWriteSkipCount(), actual.getWriteSkipCount());
        assertEquals(expected.getReadSkipCount(), actual.getReadSkipCount());
        assertEquals(expected.getProcessSkipCount(), actual.getProcessSkipCount());
        assertEquals(expected.getRollbackCount(), actual.getRollbackCount());
        assertEquals(expected.getExitStatus(), actual.getExitStatus());
        assertEquals(expected.getLastUpdated(), actual.getLastUpdated());
        assertEquals(expected.getExitStatus(), actual.getExitStatus());
        assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
    }
}
