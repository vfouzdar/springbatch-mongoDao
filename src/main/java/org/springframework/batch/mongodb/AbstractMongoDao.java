package org.springframework.batch.mongodb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.data.mongodb.core.MongoTemplate;


/**
 * Parent class for all DAO used to store SpringBatch Infrastructure data to Mongo DB.
 *  
 * @author vfouzdar
 * @author Baruch S.
 */
public abstract class AbstractMongoDao {

    
    public static final String UPDATED_EXISTING_STATUS = "updatedExisting";
    public static final String VERSION_KEY = "version";
    public static final String START_TIME_KEY = "startTime";
    public static final String END_TIME_KEY = "endTime";
    public static final String EXIT_CODE_KEY = "exitCode";
    public static final String EXIT_MESSAGE_KEY = "exitMessage";
    public static final String LAST_UPDATED_KEY = "lastUpdated";
    public static final String STATUS_KEY = "status";
    public static final String SEQUENCES_COLLECTION_NAME = "Sequences";
    public static final String ID_KEY = "_id";
    public static final String NS_KEY = "_ns";
    public static final String DOT_ESCAPE_STRING = "\\{dot\\}";
    public static final String DOT_STRING = "\\.";
    
    // Job Constants    
    public static final String JOB_NAME_KEY = "jobName";
    public static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";
    public static final String JOB_KEY_KEY = "jobKey";
    public static final String JOB_PARAMETERS_KEY = "jobParameters";
    
    // Job Execution Constants
    public static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";
	public static final String CREATE_TIME_KEY = "createTime";
	
	// Job Execution Contexts Constants
	public static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";	
	public static final String TYPE_SUFFIX = "_TYPE";

	// Step Execution Constants
    public static final String STEP_NAME_KEY = "stepName";
    public static final String COMMIT_COUNT_KEY = "commitCount";
    public static final String READ_COUNT_KEY = "readCount";
    public static final String FILTER_COUT_KEY = "filterCout";
    public static final String WRITE_COUNT_KEY = "writeCount";
    public static final String READ_SKIP_COUNT_KEY = "readSkipCount";
    public static final String WRITE_SKIP_COUNT_KEY = "writeSkipCount";
    public static final String PROCESS_SKIP_COUT_KEY = "processSkipCout";
    public static final String ROLLBACK_COUNT_KEY = "rollbackCount";
            
    protected abstract DBCollection getCollection();
    
        
    protected Long getNextId(String name, MongoTemplate mongoTemplate) {    	
        DBCollection collection = mongoTemplate.getDb().getCollection(SEQUENCES_COLLECTION_NAME);
        BasicDBObject sequence = new BasicDBObject("name", name);
        collection.update(sequence, new BasicDBObject("$inc", new BasicDBObject("value", 1L)), true, false);
        return (Long) collection.findOne(sequence).get("value");
    }

    protected void removeSystemFields(DBObject dbObject) {
        dbObject.removeField(ID_KEY);
        dbObject.removeField(NS_KEY);
    }
    
    protected BasicDBObject jobInstanceIdObj(Long id) {
        return new BasicDBObject(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY, id);
    }
    
    protected BasicDBObject jobExecutionIdObj(Long id) {
		return new BasicDBObject(JOB_EXECUTION_ID_KEY, id);
	}
    
    @SuppressWarnings({"unchecked"})
    protected JobParameters getJobParameters(Long jobInstanceId, MongoTemplate mongoTemplate) {
		 DBObject jobParamObj = mongoTemplate
				.getCollection(JobInstance.class.getSimpleName())
				.findOne(new BasicDBObject(jobInstanceIdObj(jobInstanceId)));

		if (jobParamObj != null && jobParamObj.get(MongoJobInstanceDao.JOB_PARAMETERS_KEY) != null){
			
			Map<String, ?> jobParamsMap = (Map<String, ?>) jobParamObj.get(MongoJobInstanceDao.JOB_PARAMETERS_KEY);
					
			Map<String, JobParameter> map = new HashMap<String, JobParameter>(
					jobParamsMap.size());
			for (Map.Entry<String, ?> entry : jobParamsMap.entrySet()) {
				Object param = entry.getValue();
				String key = entry.getKey().replaceAll(DOT_ESCAPE_STRING,
						DOT_STRING);
				if (param instanceof String) {
					map.put(key, new JobParameter((String) param));
				} else if (param instanceof Long) {
					map.put(key, new JobParameter((Long) param));
				} else if (param instanceof Double) {
					map.put(key, new JobParameter((Double) param));
				} else if (param instanceof Date) {
					map.put(key, new JobParameter((Date) param));
				} else {
					map.put(key, null);
				}
			}
			return new JobParameters(map);
		}
		return null;
	}
}
