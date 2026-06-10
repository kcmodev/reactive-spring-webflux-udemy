package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Random;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

        fluxAndMonoGeneratorService.namesFlux()
                .log()
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));

        fluxAndMonoGeneratorService.nameMono()
                .log()
                .subscribe(name -> System.out.println("(Mono) Name is: " + name));

        fluxAndMonoGeneratorService.namesFluxLimited(2)
                .log()
                .subscribe(name -> System.out.println("(Flux) Name is: " + name)).dispose();

        fluxAndMonoGeneratorService.namesFluxUpperCase()
                .log()
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));

        fluxAndMonoGeneratorService.splitStringAsync(3)
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));
    }

    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"));
    }

    public Flux<String> namesFluxLimited(int limit) {
        return namesFlux().limitRequest(limit);
    }

    public Flux<String> namesFluxUpperCase() {
        return namesFlux().map(String::toUpperCase);
    }

    public Flux<String> splitStringAsync(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitStringWithDelay)
                .log();
    }

    public Flux<String> splitStringWithDelay(String name) {
        return Flux.fromArray(name.split(""))
                .delayElements(Duration.ofMillis(new Random().nextInt(1000)))
                .doOnNext(s -> System.out.println("Value received: " + s))
                .map(String::toUpperCase);
    }

    public Mono<String> nameMono() {
        return Mono.just("alex");
    }
}
