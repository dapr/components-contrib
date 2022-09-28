################################################################################
# Target: e2e-tests-zeebe                                                      #
################################################################################
.PHONY: e2e-tests-zeebe
e2e-tests-zeebe:
	CGO_ENABLED=$(CGO) go test -v -tags=e2etests -count=1 ./tests/e2e/bindings/zeebe/...
