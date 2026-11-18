# Spring WebFlux Lab

A practical Spring WebFlux and Project Reactor example project.

## Run

Requires Java 17+.

```powershell
.\mvnw.cmd spring-boot:run
```

The application starts on `http://localhost:8080`.

## Examples

| Endpoint | Demonstrates |
|---|---|
| `/mono` | `Mono.just`, empty values, error recovery, and deferred factories |
| `/flux` | `Flux` creation, filtering, mapping, and collection |
| `/map` | Transforming a value with `map` |
| `/flatmap` | Dependent, concurrent, sequential, and bounded `flatMap` composition |
| `/zip` | Concurrent lookups with `Mono.zip` |
| `/sequential` | Sequential asynchronous lookups |
| `/stream` | Server-Sent Events (`text/event-stream`) |
| `/backpressure` | Buffer, drop, latest, and `limitRate` strategies |
| `/bounded-elastic` | Offloading blocking work and switching schedulers |
| `/error` | Error recovery, fallbacks, mapping, and timeout handling |

```powershell
Invoke-RestMethod http://localhost:8080/zip
Invoke-RestMethod http://localhost:8080/error
```

## Test

```powershell
.\mvnw.cmd test
```
