package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class FluxAndMonoGeneratorServiceTest {

    FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

    @Test
    void namesFluxTest() {
        // when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux();

        // then
        StepVerifier.create(namesFlux)
                .expectNext("Chris", "Steve", "Mark", "Bill")
                .verifyComplete();
    }

}