package org.springframework.batch.mongodb.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.stream.Stream;

public class MongoDataRule implements TestRule {
    private final Object test;
    private MongoTemplate template;
    private final String[] collections = {"Sequences", "JobInstance", "JobExecution", "ExecutionContext"};

    public MongoTemplate getTemplate() {
        if(this.template == null){
            this.template = setUpTemplate();
            Assert.notNull(template, "template can not be null");
        }
        return this.template;
    }

    private MongoTemplate setUpTemplate(){
        Field field = ReflectionUtils.findField(test.getClass(), "mongoTemplate");
        if(field != null) {
            field.setAccessible(true);
            Object fileldValue = ReflectionUtils.getField(field, test);
            if(fileldValue instanceof MongoTemplate)
                return (MongoTemplate) fileldValue;
        }
        return null;
    }
    public MongoDataRule(Object test) {
        this.test = test;


    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    getTemplate().getDb().drop();
                    Stream.of(collections)
                            .filter(col -> !getTemplate().collectionExists(col))
                            .forEach(col -> getTemplate().createCollection(col));
                    //getTemplate().createCollection("Sequences");
                    base.evaluate();
                }finally {

                }
            }
        };
    }
}
