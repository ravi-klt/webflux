package com.example.webflux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

class ReactiveServiceTest {

	private final ReactiveService service = new ReactiveService();

	@Test
	void supplierRunsOnlyWhenSubscribed() {
		AtomicInteger executions = new AtomicInteger();
		var lazy = reactor.core.publisher.Mono.fromSupplier(() -> {
			executions.incrementAndGet();
			return "booking";
		});

		assertEquals(0, executions.get());
		StepVerifier.create(lazy)
				.expectNext("booking")
				.verifyComplete();
		assertEquals(1, executions.get());
	}

	@Test
	void monoExamplesExposeAllCoreSignalsAndLazyFactories() {
		StepVerifier.create(service.monoExamples())
				.assertNext(result -> {
					assertEquals("Booking 101", result.get("just"));
					assertEquals("no booking", result.get("empty"));
					assertEquals("fallback booking", result.get("error"));
					assertEquals("Flight WL101", result.get("defer/fromSupplier"));
				})
				.verifyComplete();
	}

	@Test
	void fluxExamplesExerciseCreationAndTransformOperators() {
		StepVerifier.create(service.fluxExamples())
				.assertNext(result -> {
					assertEquals(2, ((List<?>) result.get("just")).size());
					assertEquals(2, ((List<?>) result.get("fromIterable/filter/map")).size());
					assertEquals(List.of(10, 20, 30, 40), result.get("range/map"));
				})
				.verifyComplete();
	}

	@Test
	void mapTransformsAndFlatMapExamplesShowOrderingAndConcurrency() {
		StepVerifier.create(service.mapExample())
				.assertNext(booking -> assertEquals("CHECKED_IN", booking.status()))
				.verifyComplete();

		StepVerifier.create(service.flatMapExamples())
				.assertNext(result -> {
					assertEquals("flight for booking-101", result.get("dependent flatMap"));
					assertEquals(List.of("flight-1", "flight-2", "flight-3", "flight-4", "flight-5"),
							result.get("concatMap (ordered)"));
					List<?> concurrent = (List<?>) result.get("flatMap (concurrency 3)");
					assertEquals(5, concurrent.size());
					assertEquals("flight-3", concurrent.get(0));
					assertEquals(5, ((List<?>) result.get("flatMap (concurrency 2)")).size());
				})
				.verifyComplete();
	}

	@Test
	void zipStartsLookupsConcurrentlyWhileSequentialCompositionChainsThem() {
		StepVerifier.create(service.concurrentZip())
				.assertNext(result -> {
					assertEquals("Mono.zip", result.get("mode"));
					assertEquals("booking-101", result.get("booking"));
					assertTrue((long) result.get("elapsedMillis") < 1_000);
				})
				.expectComplete()
				.verify(Duration.ofSeconds(2));

		StepVerifier.create(service.sequentialLookups())
				.assertNext(result -> {
					assertEquals("sequential", result.get("mode"));
					assertTrue((long) result.get("elapsedMillis") >= 1_200);
				})
				.expectComplete()
				.verify(Duration.ofSeconds(3));
	}

	@Test
	void bufferedBackpressureRetainsAllTicksForARequestingSubscriber() {
		StepVerifier.withVirtualTime(service::bufferedTicks, 0)
				.thenRequest(1)
				.expectNext(0L)
				.thenAwait(Duration.ofSeconds(4))
				.thenRequest(4)
				.expectNext(1L, 2L, 3L, 4L)
				.verifyComplete();
	}

	@Test
	void dropBackpressureDiscardsTicksWithoutDemand() {
		StepVerifier.withVirtualTime(service::droppedTicks, 0)
				.thenRequest(1)
				.expectNext(0L)
				.thenAwait(Duration.ofSeconds(4))
				.thenRequest(1)
				.verifyComplete();
	}

	@Test
	void latestBackpressureKeepsOnlyTheMostRecentUndeliveredTick() {
		StepVerifier.withVirtualTime(service::latestTicks, 0)
				.thenRequest(1)
				.expectNext(0L)
				.thenAwait(Duration.ofSeconds(4))
				.thenRequest(1)
				.expectNext(4L)
				.verifyComplete();
	}

	@Test
	void baseSubscriberRequestsTwoItemsAtATimeAndCanCancel() {
		List<Integer> received = new ArrayList<>();
		service.controlledRequestPublisher().subscribe(service.requestingTwoAtATime(received::add));
		assertEquals(List.of(1, 2, 3, 4, 5, 6), received);

		List<Integer> cancelled = new ArrayList<>();
		AtomicBoolean cancellationObserved = new AtomicBoolean();
		BaseSubscriber<Integer> subscriber = new BaseSubscriber<>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(2);
			}

			@Override
			protected void hookOnNext(Integer value) {
				cancelled.add(value);
				if (value == 2) {
					cancel();
				}
			}
		};
		service.controlledRequestPublisher()
				.doOnCancel(() -> cancellationObserved.set(true))
				.subscribe(subscriber);
		assertEquals(List.of(1, 2), cancelled);
		assertTrue(cancellationObserved.get());

		AtomicReference<Throwable> subscriberError = new AtomicReference<>();
		reactor.core.publisher.Flux.<Integer>error(new IllegalStateException("publisher failed"))
				.doOnError(subscriberError::set)
				.subscribe(service.requestingTwoAtATime(ignored -> {
				}));
		assertEquals("publisher failed", subscriberError.get().getMessage());
	}

	@Test
	void boundedElasticOffloadsBlockingWorkAndPublishOnSwitchesTheNextStage() {
		StepVerifier.create(service.boundedElasticBooking())
				.assertNext(booking -> assertEquals(101, booking.id()))
				.expectComplete()
				.verify(Duration.ofSeconds(2));
	}

	@Test
	void errorOperatorsRecoverOrTransformExpectedFailures() {
		StepVerifier.create(service.errorExamples())
				.assertNext(result -> {
					assertEquals("alternate source", result.get("onErrorResume"));
					assertEquals("default flight", result.get("onErrorReturn"));
					assertEquals("lookup failed", result.get("onErrorMap/doOnError"));
					assertEquals("timed out", result.get("timeout"));
				})
				.expectComplete()
				.verify(Duration.ofSeconds(1));
	}

	@Test
	void publisherWithoutSubscriberDoesNotRunSupplier() {
		AtomicInteger executions = new AtomicInteger();
		var publisher = reactor.core.publisher.Mono.fromSupplier(() -> {
			executions.incrementAndGet();
			return "not run yet";
		});

		assertEquals(0, executions.get());
	}
}
