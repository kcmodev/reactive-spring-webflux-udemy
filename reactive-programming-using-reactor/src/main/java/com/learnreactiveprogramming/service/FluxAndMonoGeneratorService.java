package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

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
                .subscribe(name -> System.out.println("(Flux) Name is: " + name));
    }

    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"));
    }

    public Flux<String> namesFluxLimited(int limit) {
        return namesFlux().limitRequest(limit);
    }

    public Mono<String> nameMono() {
        return Mono.just("alex");
    }
}
