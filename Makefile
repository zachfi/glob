
.PHONY: proto
proto: proto-grpc

.PHONY: proto-grpc
proto-grpc:
	@echo "=== $(PROJECT_NAME) === [ proto compile    ]: compiling protobufs:"
	$(TOOLS_CMD) buf build
	$(TOOLS_CMD) buf lint
	$(TOOLS_CMD) buf generate

include build/lint.mk
include build/release.mk
include build/drone.mk
include build/tools.mk
