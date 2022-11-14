package com.baeldung.lambda.dynamodb;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.baeldung.lambda.dynamodb.bean.PersonRequest;
import com.baeldung.lambda.dynamodb.bean.PersonResponse;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;



public class SavePersonHandler implements RequestHandler<PersonRequest, PersonResponse> {

    private AmazonDynamoDB amazonDynamoDB;

    private String DYNAMODB_TABLE_NAME = "Person";
    private Regions REGION = Regions.US_EAST_1;

    Tracer tracer = GlobalOpenTelemetry
        .getTracerProvider()
        .tracerBuilder("DynaOtelIgor") //TODO Replace with the name of your tracer
        .build();

    public PersonResponse handleRequest(PersonRequest personRequest, Context context) {
        this.initDynamoDbClient();

        persistData(personRequest);

        PersonResponse personResponse = new PersonResponse();
        personResponse.setMessage("Saved Successfully!!!");
        return personResponse;
    }

    private void persistData(PersonRequest personRequest) throws ConditionalCheckFailedException {
        Span span = tracer
        .spanBuilder("persistData") //TODO Replace with the name of your span
        .setAttribute("id", String.valueOf(personRequest.getId()))
        .setAttribute("firstName", personRequest.getFirstName())
        .setAttribute("lastName", personRequest.getLastName())
        .setAttribute("age", String.valueOf(personRequest.getAge()))
        .setAttribute("address", personRequest.getAddress())
        .startSpan();

        try (Scope scope = span.makeCurrent()) {
            Map<String, AttributeValue> attributesMap = new HashMap<>();

            attributesMap.put("id", new AttributeValue(String.valueOf(personRequest.getId())));
            attributesMap.put("firstName", new AttributeValue(personRequest.getFirstName()));
            attributesMap.put("lastName", new AttributeValue(personRequest.getLastName()));
            attributesMap.put("age", new AttributeValue(String.valueOf(personRequest.getAge())));
            attributesMap.put("address", new AttributeValue(personRequest.getAddress()));
            Span spanDynamo = tracer
            .spanBuilder("my-span") //TODO Replace with the name of your span
            .setAttribute("my-key-1", "my-value-1") //TODO Add initial attributes
            .startSpan();
            spanDynamo.makeCurrent();
            amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, attributesMap);
            spanDynamo.end();
            //TODO your code goes here
        } finally {
            span.end();
        }

        
    }

    private void initDynamoDbClient() {
        this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withRegion(REGION)
            .build();
    }
}
