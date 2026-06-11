package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

        fluxAndMonoGeneratorService.namesFlux()
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));

        fluxAndMonoGeneratorService.nameMono()
                .subscribe(name -> System.out.println("(Mono) Name is: " + name));

        fluxAndMonoGeneratorService.namesFluxLimited(2)
                .subscribe(name -> System.out.println("(Flux) Name is: " + name)).dispose();

        fluxAndMonoGeneratorService.namesFluxUpperCase()
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));

        fluxAndMonoGeneratorService.namesFluxFlatMap(3)
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));

        fluxAndMonoGeneratorService.namesFluxAsyncConcatMap(3)
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));
    }

    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe")).log();
    }

    public Flux<String> namesFluxLimited(int limit) {
        return namesFlux()
                .limitRequest(limit)
                .log();
    }

    public Flux<String> namesFluxUpperCase() {
        return namesFlux()
                .map(String::toUpperCase)
                .log();
    }

    public Flux<String> namesFluxSplitStringFlatMap(int stringLength) {
        return namesFlux()
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitString)
                .log();
    }

    public Flux<String> namesFluxFlatMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitStringWithDelay)
                .log();
    }

    public Flux<String> namesFluxAsyncConcatMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .concatMap(this::splitStringWithDelay)
                .log();
    }

    public Flux<String> namesMonoFlatMapMany(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMapMany(this::splitString)// returns flux instead of mono
                .log();
    }

    public Flux<String> namesFluxTransform(int stringLength) {

        Function<Flux<String>, Flux<String>> filterMap = name -> name
                .filter(s -> s.length() > stringLength)
                .map(String::toUpperCase);

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .transform(filterMap)
                .flatMap(this::splitString)
                .defaultIfEmpty("No name found")
                .log();
    }

    public Flux<String> namesFluxSwitchIfEmpty(int stringLength) {

        Function<Flux<String>, Flux<String>> filterMap = name -> name
                .filter(s -> s.length() > stringLength)
                .map(String::toUpperCase)
                .flatMap(this::splitString);

        var defaultFlux = Flux.just("default")
                .transform(filterMap);

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .transform(filterMap)
                .switchIfEmpty(defaultFlux)
                .log();
    }

    public Flux<String> splitString(String name) {
        return Flux.fromArray(name.split(""))
                .map(String::toUpperCase)
                .log();
    }

    public Flux<String> splitStringWithDelay(String name) {
        return Flux.fromArray(name.split(""))
                .delayElements(Duration.ofMillis(new Random().nextInt(1000)))
                .map(String::toUpperCase)
                .log();
    }

    public Mono<String> nameMono() {
        return Mono.just("alex");
    }
}
