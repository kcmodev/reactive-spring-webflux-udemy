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
        var names = fluxAndMonoGeneratorService.namesFluxFlatMap(3);

        StepVerifier.create(names)
                .expectNextCount(9)
                .verifyComplete();
    }

    @Test
    void namesFluxAsyncConcatMap() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesFluxAsyncConcatMap(3);

        StepVerifier.create(names)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }

    @Test
    void namesMonoFlatMapMany() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesMonoFlatMapMany(3);

        StepVerifier.create(names)
                .expectNext("A", "L", "E", "X")
                .verifyComplete();
    }

    @Test
    void namesFluxSplitStringFlatMap() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesFluxSplitStringFlatMap(3);

        StepVerifier.create(names)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }

    @Test
    void namesFluxTransform() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesFluxTransform(3);

        StepVerifier.create(names)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }

    @Test
    void namesFluxTransformDefault() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesFluxTransform(6);

        StepVerifier.create(names)
                .expectNext("No name found")
                .verifyComplete();
    }

    @Test
    void namesFluxSwitchIfEmpty() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.namesFluxSwitchIfEmpty(6);

        StepVerifier.create(names)
                .expectNext("D", "E", "F", "A", "U", "L", "T")
                .verifyComplete();
    }

    @Test
    void concatStreams() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.concatStreams();

        StepVerifier.create(names)
                .expectNext("alex", "ben", "chloe", "D", "E", "F")
                .verifyComplete();
    }

    @Test
    void concatWithStreams() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.concatWithStreams();

        StepVerifier.create(names)
                .expectNext("A", "B")
                .verifyComplete();
    }

    @Test
    void mergeStreams() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.mergeStreams();

        StepVerifier.create(names)
                .expectNext("alex", "D", "ben", "E", "chloe", "F")
                .verifyComplete();
    }

    @Test
    void mergeWithStream() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.mergeWithStream();

        StepVerifier.create(names)
                .expectNext("alex", "D", "ben", "E", "chloe", "F")
                .verifyComplete();
    }

    @Test
    void mergeMonoWithFlux() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.mergeMonoWithFlux();

        StepVerifier.create(names)
                .expectNext("A", "B")
                .verifyComplete();
    }

    @Test
    void mergeSequentialStreams() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.mergeSequentialStreams();

        StepVerifier.create(names)
                .expectNext("A", "B", "C", "D", "E", "F")
                .verifyComplete();
    }

    @Test
    void zipStream() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.zipStream();

        StepVerifier.create(names)
                .expectNext("AD", "BE", "CF")
                .verifyComplete();
    }

    @Test
    void zipMap() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.zipMap();

        StepVerifier.create(names)
                .expectNext("AD14", "BE25", "CF36")
                .verifyComplete();
    }

    @Test
    void zipMapWith() {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        var names = fluxAndMonoGeneratorService.zipWith();

        StepVerifier.create(names)
                .expectNext("AD", "BE", "CF")
                .verifyComplete();
    }

}