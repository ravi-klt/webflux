package com.blog.webflux.controller;

import java.util.Map;

import com.blog.webflux.model.Booking;
import com.blog.webflux.service.ReactiveService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class ReactiveController {

	private final ReactiveService reactiveService;

	public ReactiveController(ReactiveService reactiveService) {
		this.reactiveService = reactiveService;
	}

	@GetMapping("/mono")
	public Mono<Map<String, Object>> mono() {
		return reactiveService.monoExamples();
	}

	@GetMapping("/flux")
	public Mono<Map<String, Object>> flux() {
		return reactiveService.fluxExamples();
	}

	@GetMapping("/map")
	public Mono<Booking> map() {
		return reactiveService.mapExample();
	}

	@GetMapping("/flatmap")
	public Mono<Map<String, Object>> flatMap() {
		return reactiveService.flatMapExamples();
	}

	@GetMapping("/zip")
	public Mono<Map<String, Object>> zip() {
		return reactiveService.concurrentZip();
	}

	@GetMapping("/sequential")
	public Mono<Map<String, Object>> sequential() {
		return reactiveService.sequentialLookups();
	}

	@GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<ServerSentEvent<Booking>> stream() {
		return reactiveService.flightStream()
				.map(booking -> ServerSentEvent.builder(booking)
						.id(Integer.toString(booking.id()))
						.event("flight")
						.build());
	}

	@GetMapping("/backpressure")
	public Mono<Map<String, Object>> backpressure() {
		return reactiveService.backpressureExamples();
	}

	@GetMapping("/bounded-elastic")
	public Mono<Booking> boundedElastic() {
		return reactiveService.boundedElasticBooking();
	}

	@GetMapping("/error")
	public Mono<Map<String, Object>> error() {
		return reactiveService.errorExamples();
	}
}
