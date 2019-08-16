package org.springframework.batch.mongodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
	    "classpath:spring/batch/config/test-context.xml"})
public class MongoJobInstanceDaoTests {

    private static final long DATE = 777;

    @Autowired
    protected JobInstanceDao jobInstanceDao;
    
    @Autowired
    protected JobExecutionDao jobExecutionDao;
    
    @Autowired
    private MongoTemplate mongoTemplate;

    private String fooJob = "foo";

    private JobParameters fooParams = new JobParametersBuilder().addString("stringKey", "stringValue").addLong(
            "longKey", Long.MAX_VALUE).addDouble("doubleKey", Double.MAX_VALUE).addDate("dateKey", new Date(DATE))
            .toJobParameters();

    @Before
    public void onSetUp() throws Exception {
        mongoTemplate.getDb().drop();
    }

    /*
      * Create and retrieve a job instance.
      */
    @Test
    public void testCreateAndRetrieve() throws Exception {

        JobInstance fooInstance = jobInstanceDao.createJobInstance(fooJob, fooParams);
        jobExecutionDao.saveJobExecution(new JobExecution(fooInstance, fooParams));
        
        assertNotNull(fooInstance.getId());
        assertEquals(fooJob, fooInstance.getJobName());        
        
        assertEquals(fooParams, jobExecutionDao.getLastJobExecution(fooInstance).getJobParameters());

        JobInstance retrievedInstance = jobInstanceDao.getJobInstance(fooJob, fooParams);
        JobParameters retrievedParams = jobExecutionDao.getLastJobExecution(retrievedInstance).getJobParameters();
        assertEquals(fooInstance, retrievedInstance);
        assertEquals(fooJob, retrievedInstance.getJobName());
        assertEquals(fooParams, retrievedParams);

        assertEquals(Long.MAX_VALUE, retrievedParams.getLong("longKey").longValue());
        assertEquals(Double.MAX_VALUE, retrievedParams.getDouble("doubleKey"), 0.001);
        assertEquals("stringValue", retrievedParams.getString("stringKey"));
        assertEquals(new Date(DATE), retrievedParams.getDate("dateKey"));
    }

    /*
      * Create and retrieve a job instance.
      */
    @Test
    public void testCreateAndGetById() throws Exception {

        JobInstance fooInstance = jobInstanceDao.createJobInstance(fooJob, fooParams);
        jobExecutionDao.saveJobExecution(new JobExecution(fooInstance, fooParams));
        assertNotNull(fooInstance.getId());
        assertEquals(fooJob, fooInstance.getJobName());
        assertEquals(fooParams, jobExecutionDao.getLastJobExecution(fooInstance).getJobParameters());

        JobInstance retrievedInstance = jobInstanceDao.getJobInstance(fooInstance.getId());
        JobParameters retrievedParams = jobExecutionDao.getLastJobExecution(retrievedInstance).getJobParameters();
        assertEquals(fooInstance, retrievedInstance);
        assertEquals(fooJob, retrievedInstance.getJobName());
        assertEquals(fooParams, retrievedParams);

        assertEquals(Long.MAX_VALUE, retrievedParams.getLong("longKey").longValue());
        assertEquals(Double.MAX_VALUE, retrievedParams.getDouble("doubleKey"), 0.001);
        assertEquals("stringValue", retrievedParams.getString("stringKey"));
        assertEquals(new Date(DATE), retrievedParams.getDate("dateKey"));
    }

    /*
      * Create and retrieve a job instance.
      */

    
    @Test
    public void testGetMissingById() throws Exception {

        JobInstance retrievedInstance = jobInstanceDao.getJobInstance(1111111L);
        assertNull(retrievedInstance);

    }

    /*
      * Create and retrieve a job instance.
      */

    
    @Test
    public void testGetJobNames() throws Exception {

        testCreateAndRetrieve();
        List<String> jobNames = jobInstanceDao.getJobNames();
        assertFalse(jobNames.isEmpty());
        assertTrue(jobNames.contains(fooJob));

    }

    /**
     * Create and retrieve a job instance.
     *
     * @throws Exception when shit happens
     */
    
    @Test
    public void testGetLastInstances() throws Exception {

        testCreateAndRetrieve();

        // unrelated job instance that should be ignored by the query
        jobInstanceDao.createJobInstance("anotherJob", new JobParameters());

        // we need two instances of the same job to check ordering
        jobInstanceDao.createJobInstance(fooJob, new JobParameters());

        List<JobInstance> jobInstances = jobInstanceDao.getJobInstances(fooJob, 0, 2);
        assertEquals(2, jobInstances.size());
        assertEquals(fooJob, jobInstances.get(0).getJobName());
        assertEquals(fooJob, jobInstances.get(1).getJobName());
        assertEquals(Integer.valueOf(0), jobInstances.get(0).getVersion());
        assertEquals(Integer.valueOf(0), jobInstances.get(1).getVersion());

        assertTrue("Last instance should be first on the list", jobInstances.get(0).getId() > jobInstances.get(1).getId());

    }

    /**
     * Create and retrieve a job instance.
     *
     * @throws Exception when shit happens
     */
    
    @Test
    public void testGetLastInstancesPaged() throws Exception {

        testCreateAndRetrieve();

        // unrelated job instance that should be ignored by the query
        jobInstanceDao.createJobInstance("anotherJob", new JobParameters());

        // we need two instances of the same job to check ordering
        jobInstanceDao.createJobInstance(fooJob, new JobParameters());

        List<JobInstance> jobInstances = jobInstanceDao.getJobInstances(fooJob, 1, 2);
        assertEquals(1, jobInstances.size());
        assertEquals(fooJob, jobInstances.get(0).getJobName());
        assertEquals(Integer.valueOf(0), jobInstances.get(0).getVersion());

    }

    /**
     * Create and retrieve a job instance.
     *
     * @throws Exception when shit happens
     */
    
    @Test
    public void testGetLastInstancesPastEnd() throws Exception {

        testCreateAndRetrieve();

        // unrelated job instance that should be ignored by the query
        jobInstanceDao.createJobInstance("anotherJob", new JobParameters());

        // we need two instances of the same job to check ordering
        jobInstanceDao.createJobInstance(fooJob, new JobParameters());

        List<JobInstance> jobInstances = jobInstanceDao.getJobInstances(fooJob, 4, 2);
        assertEquals(0, jobInstances.size());

    }

    /**
     * Trying to create instance twice for the same job+parameters causes error
     */
    
    @Test(expected = IllegalStateException.class)
    public void testCreateDuplicateInstance() {
        jobInstanceDao.createJobInstance(fooJob, fooParams);
        jobInstanceDao.createJobInstance(fooJob, fooParams);
    }

    
    @Test
    public void testCreationAddsVersion() {

        JobInstance jobInstance = new JobInstance((long) 1, "testVersionAndId");

        assertNull(jobInstance.getVersion());

        jobInstance = jobInstanceDao.createJobInstance("testVersion", new JobParameters());

        assertNotNull(jobInstance.getVersion());
    }
    
    @Test
    public void testFindJobInstanceByExecution() {

        JobInstance jobInstance = jobInstanceDao.createJobInstance("testInstance",
                new JobParameters());
        JobExecution jobExecution = new JobExecution(jobInstance, 2L, null, null);
        jobExecutionDao.saveJobExecution(jobExecution);

        JobInstance returnedInstance = jobInstanceDao.getJobInstance(jobExecution);
        assertEquals(jobInstance, returnedInstance);
    }
    /*
    @Test
    public void testCreateJobKey() {

        JobParameters jobParameters = new JobParametersBuilder().addString(
                "foo", "bar").addString("bar", "foo").toJobParameters();
        JobInstance jobInstance = jobInstanceDao.createJobInstance("testInstance",
                jobParameters);
        JobExecution jobExecution = new JobExecution(jobInstance, jobParameters);
        jobExecutionDao.saveJobExecution(jobExecution);

        assertEquals(32, jobExecutionDao.getLastJobExecution(jobInstance).getJobParameters().getString(MongoJobInstanceDao.JOB_KEY_KEY).length());

    }

    @Test
    public void testCreateJobKeyOrdering() {

        MongoJobInstanceDao jdbcDao = (MongoJobInstanceDao) jobInstanceDao;
        JobParameters jobParameters1 = new JobParametersBuilder().addString(
                "foo", "bar").addString("bar", "foo").toJobParameters();
        
        String key1 = jdbcDao.createJobKey(jobParameters1);
        
        JobParameters jobParameters2 = new JobParametersBuilder().addString(
                "bar", "foo").addString("foo", "bar").toJobParameters();
        
        String key2 = jdbcDao.createJobKey(jobParameters2);
        assertEquals(key1, key2);

    }*/

    @Test
    public void testHexing() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] bytes = digest.digest("f78spx".getBytes("UTF-8"));
        StringBuffer output = new StringBuffer();
        for (byte bite : bytes) {
            output.append(String.format("%02x", bite));
        }
        assertEquals("Wrong hash: " + output, 32, output.length());
        String value = String.format("%032x", new BigInteger(1, bytes));
        assertEquals("Wrong hash: " + value, 32, value.length());
        assertEquals(value, output.toString());
    }

}
