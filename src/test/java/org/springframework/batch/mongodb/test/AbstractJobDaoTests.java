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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.batch.mongodb.AbstractMongoDao;
import org.springframework.batch.mongodb.MongoExecutionContextDao;
import org.springframework.batch.mongodb.MongoJobInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author vfouzdar
 * @author Dave Syer
 */
public abstract class AbstractJobDaoTests {

	protected JobInstanceDao jobInstanceDao;

	protected JobExecutionDao jobExecutionDao;

	protected JobParameters jobParameters = new JobParametersBuilder()
			.addString("job.key", "jobKey").addLong("long", (long) 1)
			.addDate("date", new Date(7)).addDouble("double", 7.7)
			.toJobParameters();

	protected JobInstance jobInstance;

	protected String jobName = "Job1";

	protected JobExecution jobExecution;

	protected Date jobExecutionStartTime = new Date(System.currentTimeMillis());

	@Autowired
	protected MongoTemplate mongoTemplate;

	/*
	 * Because AbstractTransactionalSpringContextTests is used, this method will
	 * be called by Spring to set the JobRepository.
	 */

	@Autowired
	public void setJobInstanceDao(JobInstanceDao jobInstanceDao) {
		this.jobInstanceDao = jobInstanceDao;
	}

	@Autowired
	public void setJobExecutionDao(JobExecutionDao jobExecutionDao) {
		this.jobExecutionDao = jobExecutionDao;
	}

	@Before
	public void onSetUpInTransaction() throws Exception {
		mongoTemplate.getDb().drop();
		// Create job.
		jobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);

		// Create an execution
		jobExecutionStartTime = new Date(System.currentTimeMillis());

		jobExecution = new JobExecution(jobInstance, jobParameters);
		jobExecution.setStartTime(jobExecutionStartTime);
		jobExecution.setStatus(BatchStatus.STARTED);
		jobExecutionDao.saveJobExecution(jobExecution);        
        
	}

	@Test
	public void testVersionIsNotNullForJob() throws Exception {
		assertEquals(
				0,
				getVersion(JobInstance.class.getSimpleName(),
						jobInstance.getId(),
						MongoJobInstanceDao.JOB_INSTANCE_ID_KEY));
	}

	private int getVersion(String collectionName, Long id, String idKey) {
		Document dbObject = mongoTemplate.getCollection(collectionName)
				.find(new org.bson.Document(idKey, id)).projection(new Document(AbstractMongoDao.VERSION_KEY, 1)).first();
		return dbObject == null ? 0 : (Integer) dbObject
				.get(AbstractMongoDao.VERSION_KEY);
	}

	@Test
	public void testVersionIsNotNullForJobExecution() throws Exception {
		assertEquals(
				0,
				getVersion(JobExecution.class.getSimpleName(),
						jobExecution.getId(),
						MongoExecutionContextDao.JOB_EXECUTION_ID_KEY));
	}

	@Test
	public void testFindNonExistentJob() {
		// No job should be found since it hasn't been created.
		JobInstance jobInstance = jobInstanceDao.getJobInstance(
				"nonexistentJob", jobParameters);
		assertNull(jobInstance);
	}

	@Test
	public void testFindJob() {

		JobInstance instance = jobInstanceDao.getJobInstance(jobName,
				jobParameters);
		assertNotNull(instance);
		assertTrue(jobInstance.equals(instance));

		assertEquals(jobParameters,
				jobExecutionDao.getLastJobExecution(instance)
						.getJobParameters());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFindJobWithNullRuntime() {
		jobInstanceDao.getJobInstance(null, null);
	}

	/**
	 * Test that ensures that if you create a job with a given name, then find a
	 * job with the same name, but other pieces of the identifier different, you
	 * get no result, not the existing one.
	 */
	@Test
	public void testCreateJobWithExistingName() {

		String scheduledJob = "ScheduledJob";
		jobInstanceDao.createJobInstance(scheduledJob, jobParameters);

		// Modifying the key should bring back a completely different
		// JobInstance
		JobParameters tempProps = new JobParametersBuilder().addString(
				"job.key", "testKey1").toJobParameters();

		JobInstance instance;
		instance = jobInstanceDao.getJobInstance(scheduledJob, jobParameters);
		assertNotNull(instance);

		//assertEquals(jobParameters, jobExecutionDao.getLastJobExecution(instance).getJobParameters());

		instance = jobInstanceDao.getJobInstance(scheduledJob, tempProps);
		assertNull(instance);

	}

	@Test
	public void testUpdateJobExecution() {

		jobExecution.setStatus(BatchStatus.COMPLETED);
		jobExecution.setExitStatus(ExitStatus.COMPLETED);
		jobExecution.setEndTime(new Date(System.currentTimeMillis()));
		jobExecutionDao.updateJobExecution(jobExecution);

		List<JobExecution> executions = jobExecutionDao
				.findJobExecutions(jobInstance);
		assertEquals(executions.size(), 1);
		validateJobExecution(jobExecution, executions.get(0));

	}

	@Test
	public void testSaveJobExecution() {

		List<JobExecution> executions = jobExecutionDao
				.findJobExecutions(jobInstance);
		assertEquals(executions.size(), 1);
		validateJobExecution(jobExecution, executions.get(0));
	}

	@Test(expected = NoSuchObjectException.class)
	public void testUpdateInvalidJobExecution() {
		// id is invalid
		JobExecution execution = new JobExecution(jobInstance, (long) 29432,
				null, null);
		execution.incrementVersion();
		jobExecutionDao.updateJobExecution(execution);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUpdateNullIdJobExection() {

		JobExecution execution = new JobExecution(jobInstance, null);
		jobExecutionDao.updateJobExecution(execution);
	}

	@Test
	public void testJobWithSimpleJobIdentifier() throws Exception {

		String testJob = "test";
		// Create job.
		jobInstance = jobInstanceDao.createJobInstance(testJob, jobParameters);
		Document dbObject = mongoTemplate.getCollection(
				JobInstance.class.getSimpleName()).find(
				new Document(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY,
						jobInstance.getId())).first();
		assertEquals("test", dbObject.get(MongoJobInstanceDao.JOB_NAME_KEY));
	}

	@Test
	public void testJobWithDefaultJobIdentifier() throws Exception {
		System.setProperty("DB.TRACE", "true");
		String testDefaultJob = "testDefault";
		// Create job.
		jobInstance = jobInstanceDao.createJobInstance(testDefaultJob,
				jobParameters);

		JobInstance instance = jobInstanceDao.getJobInstance(testDefaultJob,
				jobParameters);

		assertNotNull(instance);
		MongoCursor<Document> dbCursor = mongoTemplate.getCollection(
				JobInstance.class.getSimpleName())
				.find(
						new Document().append(
								MongoJobInstanceDao.JOB_INSTANCE_ID_KEY,
								instance.getId())).limit(1).cursor();
		assertNotNull(dbCursor);
		assertTrue(dbCursor.hasNext());
		Document dbObject = dbCursor.next();
		assertNotNull(dbObject);
		Document jobParamObj = dbObject.get(MongoJobInstanceDao.JOB_PARAMETERS_KEY, Document.class);
		assertNotNull(jobParamObj);
		String encodedKey = "job.key".replaceAll(
				MongoJobInstanceDao.DOT_STRING,
				MongoJobInstanceDao.DOT_ESCAPE_STRING);
		assertNotNull(jobParamObj.get(encodedKey));

		assertEquals(jobParameters.getString("job.key"), jobParamObj
				.get(encodedKey).toString());

	}

	@Test
	public void testFindJobExecutions() {

		List<JobExecution> results = jobExecutionDao
				.findJobExecutions(jobInstance);
		assertEquals(results.size(), 1);
		validateJobExecution(jobExecution, results.get(0));
	}

	@Test
	public void testFindJobsWithProperties() throws Exception {

	}

	private void validateJobExecution(JobExecution lhs, JobExecution rhs) {

		// equals operator only checks id
		assertEquals(lhs, rhs);
		assertEquals(lhs.getStartTime(), rhs.getStartTime());
		assertEquals(lhs.getEndTime(), rhs.getEndTime());
		assertEquals(lhs.getStatus(), rhs.getStatus());
		assertEquals(lhs.getExitStatus(), rhs.getExitStatus());
	}

	@Test
	public void testGetLastJobExecution() {
		JobExecution lastExecution = new JobExecution(jobInstance, null);
		lastExecution.setStatus(BatchStatus.STARTED);

		int JUMP_INTO_FUTURE = 1000; // makes sure start time is 'greatest'
		lastExecution.setCreateTime(new Date(System.currentTimeMillis()
				+ JUMP_INTO_FUTURE));
		jobExecutionDao.saveJobExecution(lastExecution);

		assertEquals(lastExecution,
				jobExecutionDao.getLastJobExecution(jobInstance));
	}

	/**
	 * Trying to create instance twice for the same job+parameters causes error
	 */
	@Test(expected = IllegalStateException.class)
	public void testCreateDuplicateInstance() {
		jobParameters = new JobParameters();
		jobInstanceDao.createJobInstance(jobName, jobParameters);
		jobInstanceDao.createJobInstance(jobName, jobParameters);
	}

	@Test
	public void testCreationAddsVersion() {
		jobInstance = jobInstanceDao.createJobInstance(
				"testCreationAddsVersion", new JobParameters());
		assertNotNull(jobInstance.getVersion());
	}

	@Test
	public void testSaveAddsVersionAndId() {

		JobExecution jobExecution = new JobExecution(jobInstance, null);

		assertNull(jobExecution.getId());
		assertNull(jobExecution.getVersion());

		jobExecutionDao.saveJobExecution(jobExecution);

		assertNotNull(jobExecution.getId());
		assertNotNull(jobExecution.getVersion());
	}

	@Test
	public void testUpdateIncrementsVersion() {
		int version = jobExecution.getVersion();

		jobExecutionDao.updateJobExecution(jobExecution);

		assertEquals(version + 1, jobExecution.getVersion().intValue());
	}
}
