local fio = require('fio')
local log = require('log')
local http_client_lib = require('http.client')
local t = require('luatest')
local Process = require('luatest.process')
local etcd_utils = require('conf.driver.etcd.utils')
local etcd_client_lib = require('conf.driver.etcd')

local DEFAULT_ENDPOINT = 'http://localhost:2379'

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

-- {{{ Setup / teardown

g.before_all(function()
    -- Show logs from the etcd transport.
    log.cfg({level = 6})

    -- Wake up etcd.
    g.etcd_datadir = fio.tempdir()
    g.etcd_process = Process:start('/usr/bin/etcd', {}, {
        ETCD_DATA_DIR = g.etcd_datadir,
        ETCD_LISTEN_CLIENT_URLS = DEFAULT_ENDPOINT,
        ETCD_ADVERTISE_CLIENT_URLS = DEFAULT_ENDPOINT,
    }, {
        output_prefix = 'etcd',
    })
    t.helpers.retrying({}, function()
        local url = DEFAULT_ENDPOINT .. '/v3/cluster/member/list'
        local response = http_client_lib.post(url)
        t.assert(response.status == 200, 'etcd started')
    end)

    -- Create a client.
    g.client = etcd_client_lib.new({
        endpoints = {DEFAULT_ENDPOINT},
        -- Uncomment for debugging.
        -- http_client = {request = {verbose = true}},
    })
end)

g.after_all(function()
    -- Tear down etcd.
    g.etcd_process:kill()
    t.helpers.retrying({}, function()
        t.assert_not(g.etcd_process:is_alive(), 'etcd is still running')
    end)
    g.etcd_process = nil
    fio.rmtree(g.etcd_datadir)

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
    print(response.kvs[1].value)
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

-- {{{ Extend client / protocol

g.test_extend_protocol = function()
    local client = etcd_client_lib.new({
        endpoints = {DEFAULT_ENDPOINT},
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
        local response = rawget(self, 'transport'):request(
            '/v3/maintenance/status', request)
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
