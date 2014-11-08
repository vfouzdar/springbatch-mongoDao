package org.springframework.batch.mongodb;


import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Job Factory responsible to provide execution/Job DAOs for storing Springbatch metadata in Mongo DB. <br/>
 * Following DAO needs to be set as property during bean definition <br/>
 * <ul>
 * <li>{@link MongoExecutionContextDao}</li>
 * <li>{@link MongoJobExecutionDao}</li>
 * <li>{@link MongoJobInstanceDao}</li>
 * <li>{@link MongoStepExecutionDao}</li>
 * </ul>  
 * 
 * @author vfouzdar
 *
 */
public class MongoJobRepositoryFactoryBean extends AbstractJobRepositoryFactoryBean implements InitializingBean {
	
	/**
	 * To store sprinbatch metadata in MongoDB this should hold an instance of {@link MongoExecutionContextDao}
	 */
    private ExecutionContextDao executionContextDao;

    /**
	 * To store sprinbatch metadata in MongoDB this should hold an instance of {@link MongoJobExecutionDao}
	 */
    private JobExecutionDao jobExecutionDao;

    /**
	 * To store sprinbatch metadata in MongoDB this should hold an instance of {@link MongoJobInstanceDao}
	 */
    private JobInstanceDao jobInstanceDao;

    /**
	 * To store sprinbatch metadata in MongoDB this should hold an instance of {@link MongoStepExecutionDao}
	 */
    private StepExecutionDao stepExecutionDao;
    
    /**
	 * Should hold an instance of {@link MongoExecutionContextDao}
	 */
	public void setExecutionContextDao(ExecutionContextDao executionContextDao) {
		this.executionContextDao = executionContextDao;
	}

	/**
	 * Should hold an instance of {@link MongoJobExecutionDao}
	 */
	public void setJobExecutionDao(JobExecutionDao jobExecutionDao) {
		this.jobExecutionDao = jobExecutionDao;
	}

	/**
	 * Should hold an instance of {@link MongoJobInstanceDao}
	 */
	public void setJobInstanceDao(JobInstanceDao jobInstanceDao) {
		this.jobInstanceDao = jobInstanceDao;
	}

	/**
	 * Should hold an instance of {@link MongoStepExecutionDao}
	 */
	public void setStepExecutionDao(StepExecutionDao stepExecutionDao) {
		this.stepExecutionDao = stepExecutionDao;
	}

	@Override
	protected JobInstanceDao createJobInstanceDao() throws Exception {
		return jobInstanceDao;
	}

	@Override
	protected JobExecutionDao createJobExecutionDao() throws Exception {
		return jobExecutionDao;
	}

	@Override
	protected StepExecutionDao createStepExecutionDao() throws Exception {
		return stepExecutionDao;
	}

	@Override
	protected ExecutionContextDao createExecutionContextDao() throws Exception {
		return executionContextDao;
	}


}
