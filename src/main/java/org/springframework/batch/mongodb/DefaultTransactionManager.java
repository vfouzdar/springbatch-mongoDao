package org.springframework.batch.mongodb;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;

public class DefaultTransactionManager implements PlatformTransactionManager {

	@Override
	public TransactionStatus getTransaction(TransactionDefinition definition)
			throws TransactionException {
		// TODO Auto-generated method stub
		return new DefaultTransactionStatus(null, true, false, false, false, null);
	}

	@Override
	public void commit(TransactionStatus status) throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback(TransactionStatus status) throws TransactionException {
		// TODO Auto-generated method stub
		
	}

	
}
