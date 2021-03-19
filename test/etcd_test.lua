local fio = require('fio')
local log = require('log')
local json = require('json')
local http_client_lib = require('http.client')
local t = require('luatest')
local Process = require('luatest.process')
local etcd_utils = require('conf.driver.etcd.utils')
local etcd_client_lib = require('conf.driver.etcd')

local g = t.group()

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

local function wait_etcd_node_started(node_id)
    local server = g.etcd_servers[node_id]
    assert(server.process ~= nil)
    t.helpers.retrying({}, function()
        log.verbose('etcd_test | waiting for etcd#%d startup', node_id)
        local url = server.client_url .. '/v3/cluster/member/list'
        local response = http_client_lib.post(url)
        t.assert(response.status == 200, 'verify that etcd started')
    end)
end

-- Waits for starting unless opts.nowait is true.
local function start_etcd_node(node_id, opts)
    local opts = opts or {}

    local server = g.etcd_servers[node_id]
    assert(server.process == nil)
    server.process = Process:start(unpack(server.start_args))
    if not opts.nowait then
        wait_etcd_node_started(node_id)
    end
end

-- Waits for stopping.
local function stop_etcd_node(node_id)
    local server = g.etcd_servers[node_id]
    assert(server.process ~= nil)
    server.process:kill()
    t.helpers.retrying({}, function()
        log.verbose('etcd_test | waiting for etcd#%d teardown', node_id)
        t.assert_not(server.process:is_alive(), 'verify that etcd stopped')
    end)
    server.process = nil
end

local function start_etcd_cluster()
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
        start_etcd_node(i, {nowait = true})
    end

    -- Wait for starting.
    for i = 1, #client_urls do
        wait_etcd_node_started(i)
    end

    g.etcd_client_urls = client_urls
end

local function stop_etcd_cluster()
    g.etcd_client_urls = nil
    for i = 1, #g.etcd_servers do
        stop_etcd_node(i)
    end
    fio.rmtree(g.etcd_datadir_root)
    g.etcd_datadir_root = nil
    g.etcd_servers = nil
end

-- }}} ETCD cluster management

-- {{{ Setup / teardown

g.before_all(function()
    -- Show logs from the etcd transport.
    --
    -- Configuring of a logger without box.cfg() call is available
    -- since tarantool-2.5.0-100-ga94a9b3fd.
    if log.cfg then
        log.cfg({level = 6})
    end

    start_etcd_cluster()

    -- Create a client.
    g.client = etcd_client_lib.new({
        endpoints = g.etcd_client_urls,
        -- Uncomment for debugging.
        -- http_client = {request = {verbose = true}},
    })
end)

g.after_all(function()
    stop_etcd_cluster()

    -- Drop the client.
    g.client = nil
end)

-- }}} Setup / teardown

-- {{{ Helpers

local function assert_response_header(header)
    etcd_utils.assert_uint64(header.cluster_id)
    etcd_utils.assert_uint64(header.member_id)
    etcd_utils.assert_int64(header.revision)
    etcd_utils.assert_uint64(header.raft_term)
end

local function assert_kv_equals(kv, exp_kv)
    local exp_key = exp_kv[1]
    local exp_value = exp_kv[2]

    t.assert_equals(kv.key, exp_key, 'verify key')
    t.assert_equals(kv.value, exp_value, 'verify value')

    t.assert_not_equals(kv.create_revision, nil, 'has create_revision')
    t.assert_not_equals(kv.mod_revision, nil, 'has mod_revision')
    t.assert_not_equals(kv.version, nil, 'has version')
    t.assert_equals(kv.lease, 0, 'has no lease')
end

local function assert_put_response(response, prev_kv)
    assert_response_header(response.header)
    if prev_kv == nil then
        t.assert_equals(response.prev_kv, nil,
            'there should be no prev_kv when it is not requested')
    else
        t.assert_not_equals(response.prev_kv, nil,
            'verify that the response contains prev_kv')
        assert_kv_equals(response.prev_kv, prev_kv)
    end
end

-- opts.exp_kvs
-- opts.exp_count (defaults to #opts.exp_kvs)
-- opts.count_only -- if request was with count_only
-- opts.keys_only -- if request was with keys_only
local function assert_range_response(response, opts)
    local exp_count = opts.exp_count or #opts.exp_kvs
    assert_response_header(response.header)
    t.assert_type(response.kvs, 'table', 'kvs type')
    if opts.count_only then
        t.assert_equals(#response.kvs, 0, 'kvs count')
    else
        t.assert_equals(#response.kvs, exp_count, 'kvs count')
        for i, exp_kv in ipairs(opts.exp_kvs) do
            if opts.keys_only then
                exp_kv = table.copy(exp_kv)
                exp_kv[2] = ''
            end
            assert_kv_equals(response.kvs[i], exp_kv)
        end
    end
    t.assert_equals(response.more, false, 'no more kvs')
    t.assert_equals(response.count, exp_count, 'range response count field')
end

local function get_kv(g, key)
    local response = g.client:range(key)
    return response.kvs[1]
end

local function verify_kv(g, key, value)
    local exp_kvs = {{key, value}}
    local response = g.client:range(key)
    assert_range_response(response, {exp_kvs = exp_kvs})
end

local function verify_no_key(g, key)
    local response = g.client:range(key)
    assert_range_response(response, {exp_kvs = {}})
end

-- Return them sorted by keys.
local function put_kvs(g, count)
    -- Get N sorted keys.
    local kvs = {}
    for i = 1, count do
        kvs[i] = {gen_key(), gen_value()}
    end
    table.sort(kvs, function(a, b) return a[1] < b[1] end)

    -- Ensure they all are different.
    for i = 1, #kvs - 1 do
        assert(kvs[i][1] < kvs[i + 1][1])
    end

    -- Put those keys.
    for _, kv in ipairs(kvs) do
        g.client:put(kv[1], kv[2])
    end

    return kvs
end

local function range_response_contains(response, kvs)
    -- Transform fetched kvs to a map for convenience.
    local res_kvs = {}
    for _, kv in ipairs(response.kvs) do
        res_kvs[kv.key] = kv.value
    end

    -- All given kvs are present in the response and have given
    -- values.
    for _, kv in ipairs(kvs) do
        local key = kv[1]
        local value = kv[2]
        t.assert_not_equals(res_kvs[key], nil)
        t.assert_equals(res_kvs[key], value)
    end
end

local function range_response_not_contains(response, kvs)
    -- Transform fetched kvs to a map for convenience.
    local res_kvs = {}
    for _, kv in ipairs(response.kvs) do
        res_kvs[kv.key] = kv.value
    end

    -- All given kvs are NOT present in the response.
    for _, kv in ipairs(kvs) do
        local key = kv[1]
        t.assert_equals(res_kvs[key], nil)
    end
end

-- Usage:
--
-- 1. assert_deleterange_response(response)
--
-- * Verify response.header.
-- * Verify that response.prev_kvs is an empty table: it is
--   expected, when we don't pass {prev_kv = true} to the
--   request.
--
-- 2. assert_deleterange_response(response, {deleted = X})
--
-- * Verify response.header.
-- * Verify response.deleted.
--
-- 3. assert_deleterange_response(response, {prev_kvs = kvs})
--
-- * Verify response.header.
-- * Verify response.deleted.
-- * Verify response.prev_kvs (disregarding the order).
--
-- 4. assert_deleterange_response(response, {deleted = N,
--        prev_kvs = kvs})
--
-- The same as case (3), but with explicit 'deleted' value.
local function assert_deleterange_response(response, opts)
    local function cmp_1(a, b)
        return a[1] < b[1]
    end

    local opts = opts or {}
    local exp_deleted = opts.deleted
    local exp_prev_kvs = opts.prev_kvs

    -- When both options are provided, they should be consistent.
    if exp_deleted ~= nil and exp_prev_kvs ~= nil then
        assert(exp_deleted == #exp_prev_kvs)
    end

    -- When only opts.prev_kvs is provided, opts.deleted is set to
    -- #opts.prev_kvs.
    if exp_deleted == nil and exp_prev_kvs ~= nil then
        exp_deleted = #exp_prev_kvs
    end

    -- When opts.prev_kvs is omitted, verify that prev_kvs is
    -- an empty table (default GRPC JSON value for an array).
    if exp_prev_kvs == nil then
        exp_prev_kvs = {}
    end

    -- Verify header.
    assert_response_header(response.header)

    -- Verify deleted.
    if exp_deleted then
        t.assert_equals(response.deleted, exp_deleted)
    end

    -- Verify prev_kv (disregarding the order).
    t.assert_equals(#response.prev_kvs, #exp_prev_kvs)
    local response_prev_kvs = {}
    for i, kv in ipairs(response.prev_kvs) do
        response_prev_kvs[i] = {kv.key, kv.value}
    end
    table.sort(response_prev_kvs, cmp_1)
    table.sort(exp_prev_kvs, cmp_1)
    t.assert_equals(response_prev_kvs, exp_prev_kvs)
end

-- }}} Helpers

-- {{{ put

-- Just put and get it back.
g.test_put_basic = function()
    local key = gen_key()
    local value = gen_value()

    -- Put a key-value.
    local response = g.client:put(key, value)
    assert_put_response(response)

    -- Get it back.
    local exp_kvs = {{key, value}}
    local response = g.client:range(key)
    assert_range_response(response, {exp_kvs = exp_kvs})
end

-- Put relatively large bytes value, which contains all 256
-- possible bytes.
--
-- It is well knows that base64 have different encoding variants:
-- with or without padding, with the standard alphabet or with URL
-- safe alphabet, with or without line feeds.
--
-- Here we verify that our encoding and decoding functions are
-- suitable for working with etcd.
g.test_put_large = function()
    -- A size that is not divisible by 3 causes padding to be
    -- added (if encoding with padding is used).
    local size = 1025
    local key = gen_key()
    local value = gen_value({size = size})

    -- Put a key-value.
    local response = g.client:put(key, value)
    assert_put_response(response)

    -- Get it back.
    local exp_kvs = {{key, value}}
    local response = g.client:range(key)
    assert_range_response(response, {exp_kvs = exp_kvs})
end

-- Verify that nil key is forbidden.
g.test_nil_key = function()
    local exp_err_msg = 'etcdserver: key is not provided'

    local value = gen_value()
    local ok, err = pcall(g.client.put, g.client, nil, value)

    t.assert_not(ok, 'error is raised')
    t.assert_type(err, 'table', 'error is a table')
    t.assert_equals(tostring(err), exp_err_msg, 'error tostring() is correct')
    t.assert_equals('x' .. err, 'x' .. exp_err_msg, "('x' .. e) is correct")
    t.assert_equals(err .. 'x', exp_err_msg .. 'x', "(e .. 'x') is correct")
    t.assert_equals(err, {
        code = 3,
        message = exp_err_msg,
        code_name = 'INVALID_ARGUMENT',
    })
end

-- Put without a value associates the default string value (an
-- empty string) with the key.
g.test_nil_value = function()
    -- Associate some value with a key.
    local key = gen_key()
    local value = gen_value()
    g.client:put(key, value)

    -- Put without a value.
    g.client:put(key)
    verify_kv(g, key, '')
end

-- TODO: When we'll support leases: add basic lease test.

-- TODO: When we'll support leases: test omitted lease: whether
-- it'll use current lease or will drop the lease? I guess the
-- latter. Don't forget to update the doc for put.

-- Verify prev_kv.
g.test_put_prev_kv = function()
    local key = gen_key()
    local value_1 = gen_value()
    g.client:put(key, value_1)

    local value_2 = gen_value()
    local response = g.client:put(key, value_2, {prev_kv = true})
    assert_put_response(response, {key, value_1})
end

-- Basic put test with {ignore_value = true}.
g.test_put_ignore_value = function()
    -- Put some value.
    local key = gen_key()
    local value = gen_value()
    local response_1 = g.client:put(key, value)
    local revision_1 = response_1.header.revision
    local kv_1 = get_kv(g, key)

    -- Update the key with ignore_value.
    local response_2 = g.client:put(key, nil, {ignore_value = true})
    local revision_2 = response_2.header.revision
    local kv_2 = get_kv(g, key)

    -- Verify that one event was generated in the etcd cluster.
    t.assert_equals(revision_1 + 1, revision_2)

    -- Verify that 'value' and 'create_revision' are not changed,
    -- but other KeyValue fields are bumped as expected.
    t.assert_equals(kv_1.key, kv_2.key)
    t.assert_equals(kv_1.create_revision, kv_2.create_revision)
    t.assert_equals(kv_1.mod_revision + 1, kv_2.mod_revision)
    t.assert_equals(kv_1.version + 1, kv_2.version)
    t.assert_equals(kv_1.value, kv_2.value)
end

-- Attempt to bump a value of a non-exist key with
-- {ignore_value = true}.
g.test_put_ignore_value_without_key = function()
    local key = gen_key()
    local ok, err = pcall(g.client.put, g.client, key, nil,
        {ignore_value = true})
    t.assert_equals(ok, false)
    t.assert_equals(err, {
        code = 3,
        message = 'etcdserver: key not found',
        code_name = 'INVALID_ARGUMENT',
    })
end

-- Pass ignore_value and value both.
g.test_put_ignore_value_with_value = function()
    -- Put some value.
    local key = gen_key()
    local value_1 = gen_value()
    g.client:put(key, value_1)

    -- Update the key with ignore_value **and value**.
    local value_2 = gen_value()
    local ok, err = pcall(g.client.put, g.client, key, value_2,
        {ignore_value = true})
    t.assert_equals(ok, false)
    t.assert_equals(err, {
        code = 3,
        message = 'etcdserver: value is provided',
        code_name = 'INVALID_ARGUMENT',
    })
end

-- Attempt to bump a value of non-exist key with
-- {ignore_lease = true}.
g.test_put_ignore_lease_without_key = function()
    local key = gen_key()
    local value = gen_value()
    local ok, err = pcall(g.client.put, g.client, key, value,
        {ignore_lease = true})
    t.assert_equals(ok, false)
    t.assert_equals(err, {
        code = 3,
        message = 'etcdserver: key not found',
        code_name = 'INVALID_ARGUMENT',
    })
end

-- }}} put

-- {{{ range

-- A single key range request is tested in `test_put_basic()`.

-- Verify that constants are present as in the module as well as
-- in the instance.
g.test_constants = function()
    t.assert_not_equals(g.client.NEXT, nil)
    t.assert_not_equals(g.client.ALL, nil)

    t.assert_equals(g.client.NEXT, etcd_client_lib.NEXT)
    t.assert_equals(g.client.ALL, etcd_client_lib.ALL)
end

-- Fetch a range of keys that matches a prefix.
g.test_range_fetch_by_prefix = function()
    local prefix_1 = gen_prefix()
    local kvs = {
        {prefix_1 .. gen_key(), gen_value()},
        {prefix_1 .. gen_key(), gen_value()},
        {prefix_1 .. gen_key(), gen_value()},
    }
    for _, kv in ipairs(kvs) do
        g.client:put(kv[1], kv[2])
    end

    -- Put some value with other prefix.
    -- Expect that it'll not appear in the range below.
    local prefix_2 = gen_prefix()
    g.client:put(prefix_2 .. gen_key(), gen_value())

    -- Sorting is necessary for comparison.
    local response = g.client:range(prefix_1, g.client.NEXT, {
        sort_order = 'ASCEND',
        sort_target = 'CREATE',
    })
    assert_range_response(response, {exp_kvs = kvs})
end

-- Fetch a range from X (inclusive) to Y (exclusive).
g.test_range_fetch_range = function()
    local kvs = put_kvs(g, 4)
    local exp_kvs = {kvs[2], kvs[3]}
    local response = g.client:range(kvs[2][1], kvs[4][1], {
        sort_order = 'ASCEND',
        sort_target = 'KEY',
    })
    -- TODO: The test assumes that other keys may not appear
    -- between fourth given ones. In fact, it depends on the keys
    -- generator and if we'll want to play with it, this
    -- assumption may fail.
    assert_range_response(response, {exp_kvs = exp_kvs})
end

-- Fetch a range without an upper key bound.
g.test_range_no_upper_bound = function()
    local kvs = put_kvs(g, 6)
    local response = g.client:range(kvs[4][1], g.client.ALL)

    -- All values above than fourth are present in the result.
    range_response_contains(response, {kvs[4], kvs[5], kvs[6]})

    -- All values equal or below fourth are NOT present in the result.
    range_response_not_contains(response, {kvs[1], kvs[2], kvs[3]})
end

-- Fetch a range without a lower key bound.
g.test_range_no_lower_bound = function()
    local kvs = put_kvs(g, 6)
    local response = g.client:range(g.client.ALL, kvs[4][1])

    -- All values lower than fourth are present in the result.
    range_response_contains(response, {kvs[1], kvs[2], kvs[3]})

    -- All values equal or above fourth are NOT present in the result.
    range_response_not_contains(response, {kvs[4], kvs[5], kvs[6]})
end

-- Fetch a particular range.
g.test_range_from_to = function()
    local kvs = put_kvs(g, 6)
    local response = g.client:range(kvs[2][1], kvs[5][1])
    range_response_contains(response, {kvs[2], kvs[3], kvs[4]})
    range_response_not_contains(response, {kvs[1], kvs[5], kvs[6]})
end

-- Fetch all keys.
g.test_range_fetch_all_keys = function()
    local kvs = put_kvs(g, 6)
    local response = g.client:range(g.client.ALL, g.client.ALL)
    range_response_contains(response, kvs)
end

g.test_range_range_end_function = function()
    -- The same as `test_range_fetch_range()`, but gives range_end
    -- as a function.
    --
    -- TODO: The test depends on the key generator implementation,
    -- see a comment in `test_range_fetch_range()`.
    local kvs = put_kvs(g, 4)
    local exp_kvs = {kvs[2], kvs[3]}
    local response = g.client:range(kvs[2][1], function(key)
        assert(key == kvs[2][1])
        return kvs[4][1]
    end, {
        sort_order = 'ASCEND',
        sort_target = 'KEY',
    })
    assert_range_response(response, {exp_kvs = exp_kvs})
end

g.test_range_unknown_key = function()
    local response = g.client:range(gen_key())
    assert_range_response(response, {exp_kvs = {}})
end

-- Fetch with a limit.
g.test_range_with_limit = function()
    put_kvs(g, 6)
    local response = g.client:range(g.client.ALL, g.client.ALL, {limit = 4})
    t.assert_equals(#response.kvs, 4)
end

-- Fetch with a revision.
g.test_range_with_revision = function()
    local key = gen_key()

    -- First put.
    local value_1 = gen_value()
    local response_1 = g.client:put(key, value_1)
    local revision_1 = response_1.header.revision
    local version_1 = 1 -- just created

    -- Second put.
    local value_2 = gen_value()
    local response_2 = g.client:put(key, value_2)
    local revision_2 = response_2.header.revision

    local response = g.client:range(key, nil, {revision = revision_1})

    -- The header revision is the newest anyway.
    t.assert_equals(response.header.revision, revision_2)

    -- But the key containst the given revision and value.
    t.assert_equals(response.kvs[1].version, version_1)
    t.assert_equals(response.kvs[1].mod_revision, revision_1)
    t.assert_equals(response.kvs[1].value, value_1)
end

-- Attempt to fetch a compacted revision gives an error.
g.test_range_with_revision_compacted = function()
    local key = gen_key()
    local response_1 = g.client:put(key, gen_value())
    local revision_1 = response_1.header.revision

    local value_2 = gen_value()
    local response_2 = g.client:put(key, value_2)
    local revision_2 = response_2.header.revision

    -- Compact everything prior to revision_2 (exclusive).
    g.client.transport:request('/v3/kv/compaction', {revision = revision_2})

    -- Attempt to fetch the compacted revision gives the error.
    local ok, err = pcall(g.client.range, g.client, key, nil,
        {revision = revision_1})
    t.assert_equals(ok, false)
    t.assert_equals(err, {
        code = 11,
        message = 'etcdserver: mvcc: required revision has been compacted',
        code_name = 'OUT_OF_RANGE',
    })

    -- While the newer revision is available.
    local exp_kvs = {{key, value_2}}
    local response = g.client:range(key, nil, {revision = revision_2})
    assert_range_response(response, {exp_kvs = exp_kvs})
end

-- TODO: Fetch with particular sortings order / target.

-- Verify that the serializable option may be passed.
g.test_range_with_serializable = function()
    local kv = {gen_key(), gen_value()}
    g.client:put(kv[1], kv[2])

    -- Just verify that the option may be passed and does not
    -- change structure of the response.
    local response = g.client:range(kv[1], nil, {serializable = true})
    assert_range_response(response, {exp_kvs = {kv}})
end

-- Fetch a range with keys_only.
g.test_range_with_keys_only = function()
    local kv = {gen_key(), gen_value()}
    g.client:put(kv[1], kv[2])

    local response = g.client:range(kv[1], nil, {keys_only = true})
    assert_range_response(response, {keys_only = true, exp_kvs = {kv}})
end

-- Verify count_only.
g.test_range_with_count_only = function()
    local key = gen_key()
    local value = gen_value()

    -- Put a key-value.
    local response = g.client:put(key, value)
    assert_put_response(response)

    -- Get it back.
    local response = g.client:range(key, nil, {count_only = true})
    assert_range_response(response, {count_only = true, exp_count = 1})
end

-- TODO: Verify min/max mod/create revisions.

-- }}} range

-- {{{ deleterange

-- Those test cases are quite similar to the :range() test cases.

-- Delete a single key.
g.test_deleterange_one_key = function()
    local key = gen_key()
    g.client:put(key, gen_value())

    local response = g.client:deleterange(key)
    assert_deleterange_response(response, key, {deleted = 1})

    verify_no_key(g, key)
end

-- Verify how {prev_kv = true} works.
g.test_deleterange_prev_kv = function()
    local key = gen_key()
    local value = gen_value()
    g.client:put(key, value)

    local response = g.client:deleterange(key)
    assert_deleterange_response(response, key, {
        deleted = 1,
        prev_kvs = {{key, value}},
    })

    verify_no_key(g, key)
end

-- Delete by a key prefix.
g.test_deleterange_by_prefix = function()
    local prefix_1 = gen_prefix()
    local kvs = {
        {prefix_1 .. gen_key(), gen_value()},
        {prefix_1 .. gen_key(), gen_value()},
        {prefix_1 .. gen_key(), gen_value()},
    }
    for _, kv in ipairs(kvs) do
        g.client:put(kv[1], kv[2])
    end

    -- Put some value with other prefix. It must not be deleted.
    local prefix_2 = gen_prefix()
    t.assert_not(prefix_2:startswith(prefix_1))
    local key_2 = prefix_2 .. gen_key()
    local value_2 = gen_value()
    g.client:put(key_2, value_2)

    local response = g.client:deleterange(prefix_1, g.client.NEXT,
        {prev_kv = true})
    assert_deleterange_response(response, {prev_kvs = kvs})

    -- Keys started with prefix_1 are deleted.
    for _, kv in ipairs(kvs) do
        verify_no_key(g, kv[1])
    end

    -- Keys started with prefix_2 are kept.
    verify_kv(g, key_2, value_2)
end

-- Delete the [X; Y) range.
g.test_deleterange_range = function()
    local kvs = put_kvs(g, 4)
    g.client:deleterange(kvs[2][1], kvs[4][1])
    verify_kv(g, kvs[1][1], kvs[1][2])
    verify_no_key(g, kvs[2][1])
    verify_no_key(g, kvs[3][1])
    verify_kv(g, kvs[4][1], kvs[4][2])
end

-- Delete the [X; +inf) range.
g.test_deleterange_no_upper_bound = function()
    local kvs = put_kvs(g, 6)
    g.client:deleterange(kvs[4][1], g.client.ALL)
    verify_kv(g, kvs[1][1], kvs[1][2])
    verify_kv(g, kvs[2][1], kvs[2][2])
    verify_kv(g, kvs[3][1], kvs[3][2])
    verify_no_key(g, kvs[4][1])
    verify_no_key(g, kvs[5][1])
    verify_no_key(g, kvs[6][1])
end

-- Delete the (-inf; Y) range.
g.test_deleterange_no_lower_bound = function()
    local kvs = put_kvs(g, 6)
    g.client:deleterange(g.client.ALL, kvs[4][1])
    verify_no_key(g, kvs[1][1])
    verify_no_key(g, kvs[2][1])
    verify_no_key(g, kvs[3][1])
    verify_kv(g, kvs[4][1], kvs[4][2])
    verify_kv(g, kvs[5][1], kvs[5][2])
    verify_kv(g, kvs[6][1], kvs[6][2])
end

-- Delete all keys.
g.test_deleterange_all_keys = function()
    local kvs = put_kvs(g, 6)
    g.client:deleterange(g.client.ALL, g.client.ALL)
    for i = 1, 6 do
        verify_no_key(g, kvs[i][1])
    end
end

-- Use a function for 'range_end' argument.
g.test_deleterange_range_end_function = function()
    -- The same as 'test_deleterange_range', but passes
    -- 'range_end' as a function.
    local kvs = put_kvs(g, 4)
    g.client:deleterange(kvs[2][1], function(key)
        assert(key == kvs[2][1])
        return kvs[4][1]
    end)
    verify_kv(g, kvs[1][1], kvs[1][2])
    verify_no_key(g, kvs[2][1])
    verify_no_key(g, kvs[3][1])
    verify_kv(g, kvs[4][1], kvs[4][2])
end

-- Delete a non-exist key.
g.test_deleterange_nonexist_key = function()
    local key = gen_key()
    local response = g.client:deleterange(key, nil, {prev_kv = true})
    t.assert_equals(response.deleted, 0)
    t.assert_equals(response.prev_kvs, {})
    verify_no_key(g, key)
end

-- }}} deleterange

-- {{{ Extend client / protocol

g.test_extend_protocol = function()
    local client = etcd_client_lib.new({
        endpoints = g.etcd_client_urls,
    })

    -- Add a message to the protocol.
    local protocol = client.protocol
    protocol:add_message('StatusRequest', {})
    protocol:add_message('StatusResponse', {
        [1] = {'ResponseHeader', 'header'},
        [2] = {'string', 'version'},
        [3] = {'int64', 'dbSize'},
        [4] = {'uint64', 'leader'},
        [5] = {'uint64', 'raftIndex'},
        [6] = {'uint64', 'raftTerm'},
        [7] = {'uint64', 'raftAppliedIndex'},
        [8] = {'repeated', 'string', 'errors'},
        [9] = {'int64', 'dbSizeInUse'},
        [10] = {'bool', 'isLearner'},
    })

    -- Add a method to the client.
    local mt = {__index = table.copy(getmetatable(client).__index)}
    mt.__index.status = function(self)
        local protocol = rawget(self, 'protocol')
        local request = protocol:encode('StatusRequest', {})
        local server_api_version = rawget(self, 'server_api_version')
        local location = ('/%s/maintenance/status'):format(server_api_version)
        local response = rawget(self, 'transport'):request(location, request)
        return protocol:decode('StatusResponse', response)
    end
    setmetatable(client, mt)

    local response = client:status()
    t.assert_not_equals(response.header, nil)
    t.assert_not_equals(response.version, nil)
    t.assert_not_equals(response.dbSize, nil)
    t.assert_not_equals(response.leader, nil)
    t.assert_not_equals(response.raftIndex, nil)
    t.assert_not_equals(response.raftTerm, nil)
    t.assert_not_equals(response.raftAppliedIndex, nil)
    t.assert_not_equals(response.errors, nil)
    t.assert_not_equals(response.dbSizeInUse, nil)
    t.assert_not_equals(response.isLearner, nil)
end

-- }}} Extend client / protocol

-- {{{ Failover

g.test_failover = function()
    -- Put a key-value.
    local key = gen_key()
    local value = gen_value()
    g.client:put(key, value)

    local function assert_response_timeout(ok, err)
        -- ETCD error.
        local exp_err_msg = 'etcdserver: request timed out'
        local exp_err = {
            code = 14,
            message = exp_err_msg,
            code_name = 'UNAVAILABLE',
        }
        t.assert_not(ok)
        t.assert_equals(tostring(err), exp_err_msg)
        t.assert_equals(err, exp_err)
    end

    local function assert_response_deadline(ok, err)
        -- ETCD error.
        local exp_err_msg = 'context deadline exceeded'
        local exp_err = {
            code = 2,
            message = exp_err_msg,
            code_name = 'UNKNOWN',
        }
        t.assert_not(ok)
        t.assert_equals(tostring(err), exp_err_msg)
        t.assert_equals(err, exp_err)
    end

    local function assert_response_network_failure(ok, err)
        -- HTTP error.
        local exp_err = {
            response = {
                status = 595,
                reason = "Couldn't connect to server",
            }
        }
        local exp_err_msg = json.encode(exp_err)
        t.assert_not(ok)
        t.assert_type(err, 'table')
        t.assert_equals(tostring(err), exp_err_msg)
        t.assert_equals('x' .. err, 'x' .. exp_err_msg)
        t.assert_equals(err .. 'x', exp_err_msg .. 'x')
        t.assert_equals(err, exp_err)
    end

    local function assert_quorum_ok()
        -- Read is successful.
        local response = g.client:range(key)
        t.assert_equals(response.kvs[1].value, value)

        -- Write is successful.
        g.client:put(gen_key(), gen_value())
    end

    local function assert_replica_ok()
        -- Serializable read is successful.
        local response = g.client:range(key, nil, {serializable = true})
        t.assert_equals(response.kvs[1].value, value)
    end

    local function assert_quorum_failure()
        -- Read fails.
        local ok, err = pcall(g.client.range, g.client, key)
        assert_response_timeout(ok, err)

        -- Write fails.
        --
        -- I don't know why the response to a write request
        -- differs from the response to a read request on an
        -- unhealthy cluster (when there is no quorum), so just
        -- tested the actual behaviour I got on etcd 3.4.14.
        local ok, err = pcall(g.client.put, g.client, gen_key(), gen_value())
        assert_response_deadline(ok, err)
    end

    local function assert_network_failure()
        -- Read fails.
        local ok, err = pcall(g.client.range, g.client, key)
        assert_response_network_failure(ok, err)

        -- Write fails.
        local ok, err = pcall(g.client.put, g.client, gen_key(), gen_value())
        assert_response_network_failure(ok, err)
    end

    -- All nodes are alive.
    assert_quorum_ok()

    -- etcd-1 down, etcd-2 up, etcd-3 up.
    stop_etcd_node(1)
    assert_quorum_ok()

    -- etcd-1 down, etcd-2 down, etcd-3 up (no quorum).
    stop_etcd_node(2)
    assert_replica_ok()
    assert_quorum_failure()

    -- etcd-1 down, etcd-2 down, etcd-3 down.
    stop_etcd_node(3)
    assert_network_failure()

    -- etcd-1 down, etcd-2 bootstrapping (waiting quorum),
    -- etcd-3 down.
    start_etcd_node(2, {nowait = true})
    assert_network_failure()

    -- etcd-1 down, etcd-2 up (no quorum), etcd-3 down.
    --
    -- (Start etcd-1 to allow etcd-2 to bootstrap, but stop it
    -- then.)
    start_etcd_node(1, {nowait = true})
    wait_etcd_node_started(1)
    wait_etcd_node_started(2)
    stop_etcd_node(1)
    assert_replica_ok()
    assert_quorum_failure()

    -- Wake up all nodes back.
    start_etcd_node(1)
    start_etcd_node(3)

    -- All nodes are alive.
    assert_quorum_ok()
end

-- }}}
