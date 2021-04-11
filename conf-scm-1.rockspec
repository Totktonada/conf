package = 'conf'
version = 'scm-1'
source  = {
    url    = 'git://github.com/tarantool/conf.git',
    branch = 'master',
}
description = {
    summary    = 'Configuration storage client',
    homepage   = 'https://github.com/tarantool/conf',
    maintainer = 'Alexander Turenko <alexander.turenko@tarantool.org>',
    license    = 'BSD2',
}
dependencies = {
    -- Don't require 'tarantool' explicitly to allow installing
    -- using plain luarocks (as opposite to tarantoolctl rocks).
    'lua >= 5.1',
}
build = {
    type = 'make',
    -- Nothing to build.
    build_pass = false,
    variables = {
        -- Support unusual tarantool location.
        --
        -- https://github.com/tarantool/modulekit/issues/2
        TARANTOOL_INSTALL_LUADIR='$(LUADIR)',
    },
    -- Don't copy doc/ folder.
    copy_directories = {},
}
