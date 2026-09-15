include project.env

DEPS = rabbit_common rabbit amqp_client rabbitmq_management prometheus rabbitmq_prometheus
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

fw:
	@if [ ! -d "src/extension-framework" ]; then \
		echo "The Seventh-State Framework was not found. Please provide it in src/extension-framework." >&2; \
		exit 1; \
	fi

package: fw
	@echo "Building packages using Seventh-State framework..."
	rm -rf $(PWD)/dist/*
	$(MAKE) -C src/extension-framework build-linux build-docker MANIFEST=$(PWD)/package/manifest.yml OUTPUT_DIR=$(PWD)/dist
