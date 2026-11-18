package com.blog.webflux.service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.blog.webflux.mode.Booking;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Service;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class ReactiveService {

	private static final List<Booking> BOOKINGS = List.of(
			new Booking(101, "Ada Lovelace", "WL101", "CONFIRMED"),
			new Booking(102, "Grace Hopper", "WL202", "CONFIRMED"),
			new Booking(103, "Katherine Johnson", "WL303", "CHECKED_IN"));

	private static void log(String message) {
		System.out.printf("[%s] %s%n", Thread.currentThread().getName(), message);
	}

	// Mono: one value, no value, or an error. These factories run when subscribed.
	public Mono<Map<String, Object>> monoExamples() {
		Mono<String> one = Mono.just("Booking 101");
		Mono<String> empty = Mono.<String>empty().defaultIfEmpty("no booking");
		Mono<String> recovered = Mono.<String>error(new IllegalStateException("not found"))
				.onErrorReturn("fallback booking");
		Mono<String> deferred = Mono.defer(() -> Mono.fromSupplier(() -> {
			log("deferred supplier runs on subscription");
			return "Flight WL101";
		}));

		return Mono.zip(one, empty, recovered, deferred)
				.map(values -> Map.of("just", values.getT1(), "empty", values.getT2(),
						"error", values.getT3(), "defer/fromSupplier", values.getT4()));
	}

	// Flux: many values, transformed synchronously, then collected for a JSON response.
	public Mono<Map<String, Object>> fluxExamples() {
		Mono<List<Booking>> fromJust = Flux.just(BOOKINGS.get(0), BOOKINGS.get(1)).collectList();
		Mono<List<Booking>> fromIterable = Flux.fromIterable(BOOKINGS)
				.filter(booking -> booking.id() != 102)
				.map(booking -> new Booking(booking.id(), booking.passenger(),
						booking.flightNumber(), "AVAILABLE"))
				.collectList();
		Mono<List<Integer>> fromRange = Flux.range(1, 4).map(number -> number * 10).collectList();

		return Mono.zip(fromJust, fromIterable, fromRange)
				.map(values -> Map.of("just", values.getT1(), "fromIterable/filter/map", values.getT2(),
						"range/map", values.getT3()));
	}

	public Flux<Booking> flightStream() {
		return Flux.interval(Duration.ZERO, Duration.ofSeconds(1))
				.map(tick -> BOOKINGS.get((int) (tick % BOOKINGS.size())))
				.doOnNext(booking -> log("SSE flight " + booking.flightNumber()));
	}

	// map changes a value. flatMap composes a value with another asynchronous publisher.
	public Mono<Booking> mapExample() {
		return Mono.just(BOOKINGS.get(0))
				.map(booking -> {
					log("map runs synchronously");
					return new Booking(booking.id(), booking.passenger(),
							booking.flightNumber(), "CHECKED_IN");
				});
	}

	private Mono<String> flightAfter(int id) {
		// Different delays make concurrent flatMap results visibly arrive out of order.
		return Mono.delay(Duration.ofMillis((6L - id) * 35))
				.map(ignored -> "flight-" + id)
				.doOnNext(value -> log("completed " + value));
	}

	public Mono<Map<String, Object>> flatMapExamples() {
		Mono<String> dependent = Mono.just(101)
				.flatMap(id -> Mono.delay(Duration.ofMillis(30)).thenReturn("flight for booking-" + id));
		Mono<List<String>> concurrent = Flux.range(1, 5).flatMap(this::flightAfter, 3).collectList();
		Mono<List<String>> sequential = Flux.range(1, 5).concatMap(this::flightAfter).collectList();
		Mono<List<String>> bounded = Flux.range(1, 5).flatMap(this::flightAfter, 2).collectList();

		return Mono.zip(dependent, concurrent, sequential, bounded)
				.map(values -> Map.of("dependent flatMap", values.getT1(),
						"flatMap (concurrency 3)", values.getT2(),
						"concatMap (ordered)", values.getT3(),
						"flatMap (concurrency 2)", values.getT4()));
	}

	private Mono<String> bookingLookup() {
		return delayedValue("booking-101", 400);
	}

	private Mono<String> flightLookup() {
		return delayedValue("flight-WL101", 600);
	}

	private Mono<String> loyaltyLookup() {
		return delayedValue("Gold member", 300);
	}

	private Mono<String> delayedValue(String value, long millis) {
		return Mono.delay(Duration.ofMillis(millis))
				.doOnSubscribe(ignored -> log("start " + value + " (" + millis + " ms)"))
				.thenReturn(value);
	}

	private Map<String, Object> timing(String mode, long startedAt, String booking,
			String flight, String loyalty) {
		long elapsed = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();
		log(mode + " completed in " + elapsed + " ms");
		return Map.of("mode", mode, "booking", booking, "flight", flight,
				"loyalty", loyalty, "elapsedMillis", elapsed);
	}

	public Mono<Map<String, Object>> sequentialLookups() {
		return Mono.defer(() -> {
			long startedAt = System.nanoTime();
			return bookingLookup().flatMap(booking -> flightLookup().flatMap(flight ->
					loyaltyLookup().map(loyalty ->
							timing("sequential", startedAt, booking, flight, loyalty))));
		});
	}

	public Mono<Map<String, Object>> concurrentZip() {
		return Mono.defer(() -> {
			long startedAt = System.nanoTime();
			return Mono.zip(bookingLookup(), flightLookup(), loyaltyLookup())
					.map(values -> timing("Mono.zip", startedAt,
							values.getT1(), values.getT2(), values.getT3()));
		});
	}

	// With no demand, buffer keeps values, drop discards them, latest keeps only the newest.
	public Flux<Long> bufferedTicks() {
		return ticks().onBackpressureBuffer().doOnNext(value -> log("buffer delivered " + value));
	}

	public Flux<Long> droppedTicks() {
		return ticks().onBackpressureDrop(value -> log("drop discarded " + value))
				.doOnNext(value -> log("drop delivered " + value));
	}

	public Flux<Long> latestTicks() {
		return ticks().onBackpressureLatest().doOnNext(value -> log("latest delivered " + value));
	}

	private Flux<Long> ticks() {
		return Flux.interval(Duration.ZERO, Duration.ofSeconds(1)).take(5);
	}

	public Mono<Map<String, Object>> backpressureExamples() {
		// An asynchronous producer outruns delayElements; limitRate only batches upstream requests.
		Flux<Long> fast = Flux.interval(Duration.ZERO, Duration.ofMillis(2)).take(60);
		Mono<List<Long>> buffer = consume(fast.onBackpressureBuffer(), "buffer");
		Mono<List<Long>> drop = consume(fast.onBackpressureDrop(), "drop");
		Mono<List<Long>> latest = consume(fast.onBackpressureLatest(), "latest");
		Mono<List<Integer>> limitRate = Flux.range(1, 20)
				.doOnRequest(size -> log("upstream request(" + size + ")"))
				.limitRate(5).collectList();

		return Mono.zip(buffer, drop, latest, limitRate)
				.map(values -> Map.of("buffer", values.getT1(), "drop", values.getT2(),
						"latest", values.getT3(), "limitRate", values.getT4()));
	}

	private Mono<List<Long>> consume(Flux<Long> source, String name) {
		return source.doOnNext(value -> log(name + " produced " + value))
				.delayElements(Duration.ofMillis(12))
				.doOnNext(value -> log(name + " consumed " + value))
				.collectList();
	}

	public Flux<Integer> controlledRequestPublisher() {
		return Flux.range(1, 6)
				.doOnRequest(size -> log("publisher request(" + size + ")"))
				.doOnNext(value -> log("publisher emitted " + value));
	}

	public BaseSubscriber<Integer> requestingTwoAtATime(Consumer<Integer> onNext) {
		return new BaseSubscriber<>() {
			private int received;

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				log("subscribed; request(2)");
				request(2);
			}

			@Override
			protected void hookOnNext(Integer value) {
				log("onNext(" + value + ")");
				onNext.accept(value);
				if (++received % 2 == 0) {
					request(2);
				}
			}

			@Override
			protected void hookOnError(Throwable error) {
				log("onError: " + error.getMessage());
			}

			@Override
			protected void hookOnComplete() {
				log("onComplete");
			}
		};
	}

	// Blocking legacy call: boundedElastic protects the WebFlux event-loop threads.
	private Booking blockingBookingCall() throws InterruptedException {
		log("blocking call starts");
		Thread.sleep(100);
		log("blocking call ends");
		return BOOKINGS.get(0);
	}

	public Mono<Booking> boundedElasticBooking() {
		return Mono.fromCallable(this::blockingBookingCall)
				.subscribeOn(Schedulers.boundedElastic())
				.doOnNext(booking -> log("subscribeOn: " + booking.flightNumber()))
				.publishOn(Schedulers.parallel())
				.doOnNext(booking -> log("publishOn: " + booking.flightNumber()));
	}

	public Mono<Map<String, Object>> errorExamples() {
		Mono<String> resume = Mono.<String>error(new IllegalStateException("unavailable"))
				.onErrorResume(error -> Mono.just("alternate source"));
		Mono<String> fallback = Mono.<String>error(new IllegalArgumentException("invalid flight"))
				.onErrorReturn("default flight");
		Mono<String> mapped = Mono.<String>error(new IllegalStateException("database failure"))
				.onErrorMap(error -> new RuntimeException("lookup failed", error))
				.doOnError(error -> log("onErrorMap/doOnError: " + error.getMessage()))
				.onErrorResume(error -> Mono.just(error.getMessage()));
		Mono<String> timeout = Mono.delay(Duration.ofMillis(100)).thenReturn("late")
				.timeout(Duration.ofMillis(20))
				.onErrorResume(TimeoutException.class, error -> Mono.just("timed out"));

		return Mono.zip(resume, fallback, mapped, timeout)
				.map(values -> Map.of("onErrorResume", values.getT1(),
						"onErrorReturn", values.getT2(), "onErrorMap/doOnError", values.getT3(),
						"timeout", values.getT4()));
	}
}
