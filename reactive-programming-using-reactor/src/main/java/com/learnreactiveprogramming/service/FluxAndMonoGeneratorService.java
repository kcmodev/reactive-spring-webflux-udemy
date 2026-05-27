package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class FluxAndMonoGeneratorService {

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

        fluxAndMonoGeneratorService.namesFlux()
                .subscribe(name -> System.out.println("Flux: " + name));

        fluxAndMonoGeneratorService.nameMono()
                .subscribe(name -> System.out.println("Mono: " + name));
    }

    public Mono<String> nameMono() {
        return Mono.just("Owen")
                .log();
    }


    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("Chris", "Steve", "Mark", "Bill"))
                .log();
    }
}
