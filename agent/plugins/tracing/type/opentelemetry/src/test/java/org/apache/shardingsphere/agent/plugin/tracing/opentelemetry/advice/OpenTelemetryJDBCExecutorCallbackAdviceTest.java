/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.agent.plugin.tracing.opentelemetry.advice;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.agent.api.advice.TargetAdviceObject;
import org.apache.shardingsphere.agent.plugin.tracing.core.RootSpanContext;
import org.apache.shardingsphere.agent.plugin.tracing.core.constant.AttributeConstants;
import org.apache.shardingsphere.agent.plugin.tracing.opentelemetry.constant.OpenTelemetryConstants;
import org.apache.shardingsphere.agent.plugin.tracing.opentelemetry.fixture.JDBCExecutorCallbackFixture;
import org.apache.shardingsphere.database.connector.core.jdbcurl.parser.ConnectionProperties;
import org.apache.shardingsphere.database.connector.core.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dml.SelectStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.internal.configuration.plugins.Plugins;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OpenTelemetryJDBCExecutorCallbackAdviceTest {
    
    public static final String DATA_SOURCE_NAME = "mock.db";
    
    public static final String SQL = "SELECT 1";
    
    private static final String DB_TYPE = "SQL92";
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "FIXTURE");
    
    private final InMemorySpanExporter testExporter = InMemorySpanExporter.create();
    
    private Span parentSpan;
    
    private Object previousRootSpan;
    
    private SdkTracerProvider tracerProvider;
    
    private TargetAdviceObject targetObject;
    
    private JDBCExecutionUnit executionUnit;
    
    @BeforeEach
    void setup() {
        tracerProvider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(testExporter)).build();
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal().getTracer(OpenTelemetryConstants.TRACER_NAME);
        parentSpan = GlobalOpenTelemetry.getTracer(OpenTelemetryConstants.TRACER_NAME).spanBuilder("parent").startSpan();
        previousRootSpan = RootSpanContext.get();
        RootSpanContext.set(parentSpan);
        prepare();
    }
    
    @SuppressWarnings("rawtypes")
    @SneakyThrows({ReflectiveOperationException.class, SQLException.class})
    private void prepare() {
        Statement statement = mock(Statement.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(databaseMetaData.getURL()).thenReturn("mock_url");
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(statement.getConnection()).thenReturn(connection);
        executionUnit = new JDBCExecutionUnit(new ExecutionUnit(DATA_SOURCE_NAME, new SQLUnit(SQL, Collections.emptyList())), null, statement);
        ResourceMetaData resourceMetaData = mock(ResourceMetaData.class, RETURNS_DEEP_STUBS);
        when(resourceMetaData.getStorageUnits().get(DATA_SOURCE_NAME).getStorageType()).thenReturn(TypedSPILoader.getService(DatabaseType.class, "SQL92"));
        when(resourceMetaData.getStorageUnits().get(DATA_SOURCE_NAME).getConnectionProperties()).thenReturn(mock(ConnectionProperties.class));
        JDBCExecutorCallback jdbcExecutorCallback = new JDBCExecutorCallbackFixture(
                TypedSPILoader.getService(DatabaseType.class, "SQL92"), resourceMetaData, SelectStatement.builder().databaseType(databaseType).build(), true);
        Plugins.getMemberAccessor().set(JDBCExecutorCallback.class.getDeclaredField("resourceMetaData"), jdbcExecutorCallback, resourceMetaData);
        targetObject = (TargetAdviceObject) jdbcExecutorCallback;
    }
    
    @AfterEach
    void clean() {
        parentSpan.end();
        tracerProvider.close();
        GlobalOpenTelemetry.resetForTest();
        RootSpanContext.set(previousRootSpan);
        testExporter.reset();
    }
    
    @Test
    void assertMethod() {
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] args = new Object[]{executionUnit, false};
        advice.beforeMethod(targetObject, null, args, "OpenTelemetry");
        advice.afterMethod(targetObject, null, args, null, "OpenTelemetry");
        List<SpanData> spanItems = testExporter.getFinishedSpanItems();
        assertCommonData(spanItems, parentSpan.getSpanContext().getSpanId());
        assertThat(spanItems.iterator().next().getStatus().getStatusCode(), is(StatusCode.OK));
    }
    
    @Test
    void assertMethodWithoutParentSpan() {
        RootSpanContext.set(null);
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] args = new Object[]{executionUnit, false};
        advice.beforeMethod(targetObject, null, args, "OpenTelemetry");
        advice.afterMethod(targetObject, null, args, null, "OpenTelemetry");
        List<SpanData> spanItems = testExporter.getFinishedSpanItems();
        assertCommonData(spanItems, SpanId.getInvalid());
    }
    
    @Test
    void assertExceptionHandle() {
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] args = new Object[]{executionUnit, false};
        advice.beforeMethod(targetObject, null, args, "OpenTelemetry");
        advice.onThrowing(targetObject, null, args, new IOException(""), "OpenTelemetry");
        advice.afterMethod(targetObject, null, args, null, "OpenTelemetry");
        List<SpanData> spanItems = testExporter.getFinishedSpanItems();
        assertCommonData(spanItems, parentSpan.getSpanContext().getSpanId());
        assertThat(spanItems.iterator().next().getStatus().getStatusCode(), is(StatusCode.ERROR));
    }
    
    @Test
    void assertExceptionSpanEndedOnce() {
        Span span = mock(Span.class, Answers.RETURNS_SELF);
        SpanBuilder spanBuilder = mock(SpanBuilder.class, Answers.RETURNS_SELF);
        when(spanBuilder.startSpan()).thenReturn(span);
        Tracer tracer = mock(Tracer.class);
        when(tracer.spanBuilder("/ShardingSphere/executeSQL/")).thenReturn(spanBuilder);
        OpenTelemetry openTelemetry = mock(OpenTelemetry.class, RETURNS_DEEP_STUBS);
        when(openTelemetry.getTracerProvider().get(OpenTelemetryConstants.TRACER_NAME)).thenReturn(tracer);
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(openTelemetry);
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] args = new Object[]{executionUnit, false};
        IOException expected = new IOException("foo_failure");
        advice.beforeMethod(targetObject, null, args, "OpenTelemetry");
        advice.onThrowing(targetObject, null, args, expected, "OpenTelemetry");
        advice.afterMethod(targetObject, null, args, null, "OpenTelemetry");
        verify(span).setStatus(StatusCode.ERROR);
        verify(span).recordException(expected);
        verify(span).end();
        verifyNoMoreInteractions(span);
    }
    
    @Test
    void assertConcurrentInvocations() throws Exception {
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] firstArgs = new Object[]{executionUnit, false};
        Object[] secondArgs = new Object[]{new JDBCExecutionUnit(new ExecutionUnit(DATA_SOURCE_NAME, new SQLUnit("SELECT 2", Collections.emptyList())), null, mock(Statement.class)), false};
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch firstCompleted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> {
                advice.beforeMethod(targetObject, null, firstArgs, "OpenTelemetry");
                firstStarted.countDown();
                await(secondStarted);
                advice.afterMethod(targetObject, null, firstArgs, null, "OpenTelemetry");
                firstCompleted.countDown();
            });
            Future<?> second = executor.submit(() -> {
                await(firstStarted);
                advice.beforeMethod(targetObject, null, secondArgs, "OpenTelemetry");
                secondStarted.countDown();
                await(firstCompleted);
                advice.onThrowing(targetObject, null, secondArgs, new IOException("foo_failure"), "OpenTelemetry");
                advice.afterMethod(targetObject, null, secondArgs, null, "OpenTelemetry");
            });
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            List<SpanData> actual = testExporter.getFinishedSpanItems();
            assertThat(actual.size(), is(2));
            assertThat(actual.get(0).getSpanId(), not(actual.get(1).getSpanId()));
            assertThat(actual.get(0).getAttributes().get(AttributeKey.stringKey(AttributeConstants.DB_STATEMENT)), is(SQL));
            assertThat(actual.get(0).getStatus().getStatusCode(), is(StatusCode.OK));
            assertTrue(actual.get(0).getEvents().isEmpty());
            assertThat(actual.get(1).getAttributes().get(AttributeKey.stringKey(AttributeConstants.DB_STATEMENT)), is("SELECT 2"));
            assertThat(actual.get(1).getStatus().getStatusCode(), is(StatusCode.ERROR));
            assertThat(actual.get(1).getEvents().size(), is(1));
            assertThat(actual.get(1).getEvents().get(0).getAttributes().get(AttributeKey.stringKey("exception.message")), is("foo_failure"));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
    
    @SneakyThrows(InterruptedException.class)
    private void await(final CountDownLatch latch) {
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
    
    @Test
    void assertNestedInvocations() {
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] outerArgs = new Object[]{executionUnit, false};
        Object[] innerArgs = new Object[]{executionUnit, false};
        advice.beforeMethod(targetObject, null, outerArgs, "OpenTelemetry");
        advice.beforeMethod(targetObject, null, innerArgs, "OpenTelemetry");
        advice.onThrowing(targetObject, null, innerArgs, new IOException("foo_failure"), "OpenTelemetry");
        advice.afterMethod(targetObject, null, innerArgs, null, "OpenTelemetry");
        assertThat(testExporter.getFinishedSpanItems().size(), is(1));
        advice.afterMethod(targetObject, null, outerArgs, null, "OpenTelemetry");
        List<SpanData> actual = testExporter.getFinishedSpanItems();
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0).getSpanId(), not(actual.get(1).getSpanId()));
        assertThat(actual.get(0).getStatus().getStatusCode(), is(StatusCode.ERROR));
        assertThat(actual.get(1).getStatus().getStatusCode(), is(StatusCode.OK));
        assertTrue(actual.get(1).getEvents().isEmpty());
    }
    
    @Test
    void assertNestedBeforeFailure() {
        OpenTelemetryJDBCExecutorCallbackAdvice advice = new OpenTelemetryJDBCExecutorCallbackAdvice();
        Object[] outerArgs = new Object[]{executionUnit, false};
        JDBCExecutionUnit failedExecutionUnit = mock(JDBCExecutionUnit.class);
        when(failedExecutionUnit.getExecutionUnit()).thenThrow(IllegalStateException.class);
        Object[] innerArgs = new Object[]{failedExecutionUnit, false};
        advice.beforeMethod(targetObject, null, outerArgs, "OpenTelemetry");
        assertThrows(IllegalStateException.class, () -> advice.beforeMethod(targetObject, null, innerArgs, "OpenTelemetry"));
        advice.afterMethod(targetObject, null, innerArgs, null, "OpenTelemetry");
        assertTrue(testExporter.getFinishedSpanItems().isEmpty());
        advice.afterMethod(targetObject, null, outerArgs, null, "OpenTelemetry");
        assertCommonData(testExporter.getFinishedSpanItems(), parentSpan.getSpanContext().getSpanId());
    }
    
    @Test
    void assertAfterMethodWithoutSpan() {
        new OpenTelemetryJDBCExecutorCallbackAdvice().afterMethod(targetObject, null, new Object[]{executionUnit, false}, null, "OpenTelemetry");
        assertTrue(testExporter.getFinishedSpanItems().isEmpty());
    }
    
    @Test
    void assertOnThrowingWithoutSpan() {
        new OpenTelemetryJDBCExecutorCallbackAdvice().onThrowing(targetObject, null, new Object[]{executionUnit, false}, new IOException("foo_failure"), "OpenTelemetry");
        assertTrue(testExporter.getFinishedSpanItems().isEmpty());
    }
    
    private void assertCommonData(final List<SpanData> spanItems, final String expectedParentSpanId) {
        assertThat(spanItems.size(), is(1));
        SpanData spanData = spanItems.iterator().next();
        assertThat(spanData.getName(), is("/ShardingSphere/executeSQL/"));
        assertThat(spanData.getParentSpanId(), is(expectedParentSpanId));
        Attributes attributes = spanData.getAttributes();
        assertThat(attributes.get(AttributeKey.stringKey(AttributeConstants.COMPONENT)), is(AttributeConstants.COMPONENT_NAME));
        assertThat(attributes.get(AttributeKey.stringKey(AttributeConstants.DB_TYPE)), is(DB_TYPE));
        assertThat(attributes.get(AttributeKey.stringKey(AttributeConstants.DB_INSTANCE)), is(DATA_SOURCE_NAME));
        assertThat(attributes.get(AttributeKey.stringKey(AttributeConstants.DB_STATEMENT)), is(SQL));
    }
}
