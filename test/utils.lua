local fio = require('fio')
local log = require('log')
local http_client_lib = require('http.client')
local luatest_process = require('luatest.process')
local luatest_helpers = require('luatest.helpers')
local luatest_assertions = require('luatest.assertions')

-- {{{ Data generators

local kv_next = 1

local function gen_prefix()
    local res = 'prefix_' .. tostring(kv_next) .. '.'
    kv_next = kv_next + 1
    return res
end

local function gen_key()
    local res = 'key_' .. tostring(kv_next)
    kv_next = kv_next + 1
    return res
end

local function gen_value(opts)
    local opts = opts or {}
    local res = 'value_' .. tostring(kv_next)
    if opts.size then
        res = res .. '_'
        for i = 1, opts.size - string.len(res) do
            res = res .. string.char(i % 256)
        end
    end
    kv_next = kv_next + 1
    return res
end

-- }}} Data generators

-- {{{ ETCD cluster management

-- Reads g.etcd_servers[node_id].
local function wait_etcd_node_started(g, node_id)
    local server = g.etcd_servers[node_id]
    assert(server.process ~= nil)
    luatest_helpers.retrying({}, function()
        log.verbose('etcd_test | waiting for etcd#%d startup', node_id)
        local url = server.client_url .. '/v3/cluster/member/list'
        local response = http_client_lib.post(url)
        luatest_assertions.assert(response.status == 200,
            'verify that etcd started')
    end)
end

-- Waits for starting unless opts.nowait is true.
--
-- Reads g.etcd_servers[node_id].
local function start_etcd_node(g, node_id, opts)
    local opts = opts or {}

    local server = g.etcd_servers[node_id]
    assert(server.process == nil)
    server.process = luatest_process:start(unpack(server.start_args))
    if not opts.nowait then
        wait_etcd_node_started(g, node_id)
    end
end

-- Waits for stopping.
--
-- Reads g.etcd_servers[node_id].
-- Writes g.etcd_servers[node_id].process.
local function stop_etcd_node(g, node_id)
    local server = g.etcd_servers[node_id]
    assert(server.process ~= nil)
    server.process:kill()
    luatest_helpers.retrying({}, function()
        log.verbose('etcd_test | waiting for etcd#%d teardown', node_id)
        luatest_assertions.assert_not(server.process:is_alive(),
            'verify that etcd stopped')
    end)
    server.process = nil
end

-- Writes g.etcd_servers.
-- Writes g.etcd_datadir_root.
-- Writes g.etcd_client_urls.
local function start_etcd_cluster(g)
    local client_urls = {
        'http://localhost:2379',
        'http://localhost:2381',
        'http://localhost:2383',
    }
    local peer_urls = {
        'http://localhost:2380',
        'http://localhost:2382',
        'http://localhost:2384',
    }

    local initial_cluster = table.concat({
        ('test1=%s'):format(peer_urls[1]),
        ('test2=%s'):format(peer_urls[2]),
        ('test3=%s'):format(peer_urls[3]),
    }, ',')

    -- Initialize nodes parameters.
    g.etcd_servers = {}
    g.etcd_datadir_root = fio.tempdir()
    for i = 1, #client_urls do
        local name = ('test%d'):format(i)
        local datadir = ('%s/%s'):format(g.etcd_datadir_root, name)
        local env = {
            ETCD_NAME = name,
            ETCD_DATA_DIR = datadir,
            ETCD_LISTEN_CLIENT_URLS = client_urls[i],
            ETCD_ADVERTISE_CLIENT_URLS = client_urls[i],
            -- Clustering.
            ETCD_LISTEN_PEER_URLS = peer_urls[i],
            ETCD_INITIAL_ADVERTISE_PEER_URLS = peer_urls[i],
            ETCD_INITIAL_CLUSTER = initial_cluster,
            ETCD_INITIAL_CLUSTER_STATE = 'new',
        }
        local start_args = {'/usr/bin/etcd', {}, env, {
            output_prefix = ('etcd #%d'):format(i),
        }}

        g.etcd_servers[i] = {}
        g.etcd_servers[i].client_url = client_urls[i]
        g.etcd_servers[i].datadir = datadir
        g.etcd_servers[i].start_args = start_args
    end

    -- Wake up nodes.
    for i = 1, #client_urls do
        start_etcd_node(g, i, {nowait = true})
    end

    -- Wait for starting.
    for i = 1, #client_urls do
        wait_etcd_node_started(g, i)
    end

    g.etcd_client_urls = client_urls
end

-- Reads g.etcd_servers.
-- Writes g.etcd_client_urls.
-- Writes g.etcd_datadir_root.
-- Writes g.etcd_servers.
local function stop_etcd_cluster(g)
    g.etcd_client_urls = nil
    for i = 1, #g.etcd_servers do
        stop_etcd_node(g, i)
    end
    fio.rmtree(g.etcd_datadir_root)
    g.etcd_datadir_root = nil
    g.etcd_servers = nil
end

-- }}} ETCD cluster management

-- {{{ Setup / teardown

-- Writes g.storage.
-- Writes g.client_urls.
-- Writes storage specific g.<...> fields.
local function before_all_default(g, opts)
    local opts = opts or {}

    if opts.storage == nil then
        error('storage is the mandatory option')
    end

    if type(opts.storage) ~= 'string' then
        error(('storage option must me a string, got %s'):format(
            type(opts.storage)))
    end

    -- Show logs from the etcd transport.
    --
    -- Configuring of a logger without box.cfg() call is available
    -- since tarantool-2.5.0-100-ga94a9b3fd.
    if log.cfg then
        log.cfg({level = 6})
    end

    if opts.storage == 'etcd' then
        start_etcd_cluster(g)
        g.client_urls = g.etcd_client_urls
    else
        error(('Unsupported storage: %s'):format(opts.storage))
    end

    g.storage = opts.storage
end

-- Reads / writes g.storage.
-- Writes g.client_urls.
-- Writes storage specific g.<...> fields.
local function after_all_default(g)
    assert(g.storage ~= nil)
    assert(type(g.storage) == 'string')

    if g.storage == 'etcd' then
        stop_etcd_cluster(g)
    else
        error(('Unsupported storage: %s'):format(g.storage))
    end
    g.client_urls = nil
    g.storage = nil
end

-- }}} Setup / teardown

return {
    gen_prefix = gen_prefix,
    gen_key = gen_key,
    gen_value = gen_value,
    wait_etcd_node_started = wait_etcd_node_started,
    start_etcd_node = start_etcd_node,
    stop_etcd_node = stop_etcd_node,
    start_etcd_cluster = start_etcd_cluster,
    stop_etcd_cluster = stop_etcd_cluster,
    before_all_default = before_all_default,
    after_all_default = after_all_default,
}
