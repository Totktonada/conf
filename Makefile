WWW_BROWSER=$(shell \
	{ type xdg-open >/dev/null 2>&1 && echo "xdg-open"; } || \
	{ type open >/dev/null 2>&1 && echo "open"; } \
)

# https://stackoverflow.com/a/18137056/1598057
#
# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))

# Add .rocks/bin to PATH.
#
# Look for .rocks/bin directories upward starting from the project
# directory.
#
# It is useful for luacheck and luatest.
#
# Note: The PROJECT_DIR holds a real path.
define ENABLE_ROCKS_BIN
	$(if $(wildcard $1/.rocks/bin),
		$(eval PATH := $(PATH):$1/.rocks/bin)
	)
	$(if $1,
		$(eval $(call ENABLE_ROCKS_BIN,$(patsubst %/,%,$(dir $1))))
	)
endef
$(eval $(call ENABLE_ROCKS_BIN,$(PROJECT_DIR)))

.PHONY: default
default:
	false

# The template (ldoc.tpl) is written using tarantool specific
# functions like string.split(), string.endswith(), so we run
# ldoc using tarantool.
.PHONY: apidoc
apidoc:
	tarantool -e "                                     \
		arg = {                                        \
			[0] = 'tarantool',                         \
			'-c', '$(PROJECT_DIR)/doc/ldoc/config.ld', \
			'-d', '$(PROJECT_DIR)/doc/apidoc',         \
			'-p', 'conf',                              \
			'$(PROJECT_DIR)'                           \
		}                                              \
		require('ldoc')                                \
		os.exit()"

.PHONY: serve-apidoc
serve-apidoc: apidoc
	$(WWW_BROWSER) $(PROJECT_DIR)/doc/apidoc/index.html

.PHONY: lint
lint:
	cd $(PROJECT_DIR) && luacheck . -r --exclude-files .rocks

.PHONY: test
test:
	cd $(PROJECT_DIR) && luatest -v
