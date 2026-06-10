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
}