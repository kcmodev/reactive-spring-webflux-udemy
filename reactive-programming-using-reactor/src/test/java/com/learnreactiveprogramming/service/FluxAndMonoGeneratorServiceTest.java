package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class FluxAndMonoGeneratorServiceTest {

    @Test
    void mainTest() {
        FluxAndMonoGeneratorService.main(null);
    }

    @Test
    void namesFluxTest() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        StepVerifier.create(fluxAndMonoGeneratorService.namesFlux())
                .expectNext("alex", "ben", "chloe")
                .verifyComplete();
    }

    @Test
    void namesFluxLimitedTest() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        StepVerifier.create(fluxAndMonoGeneratorService.namesFluxLimited(2))
                .expectNext("alex", "ben")
                .verifyComplete();
    }

    @Test
    void nameMonoTest() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        StepVerifier.create(fluxAndMonoGeneratorService.nameMono())
                .expectNext("alex")
                .verifyComplete();
    }

    @Test
    void namesFluxUpperCase() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        StepVerifier.create(fluxAndMonoGeneratorService.namesFluxUpperCase())
                .expectNext("ALEX", "BEN", "CHLOE")
                .verifyComplete();
    }

    @Test
    void splitStringWithDelay() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.splitStringAsync(3);

        StepVerifier.create(names)
                .expectNextCount(9)
                .verifyComplete();
    }

    @Test
    void splitStringAsyncConcatMap() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.splitStringAsyncConcatMap(3);

        StepVerifier.create(names)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }
}