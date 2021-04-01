local conf_lib = require('conf')
local t = require('luatest')
local test_utils = require('test.utils')

local g = t.group()

-- Shortcuts.
local before_all_default = test_utils.before_all_default
local after_all_default = test_utils.after_all_default

-- {{{ Setup / teardown

g.before_all(function()
    -- TODO: Run those tests against several storages.
    local storage = 'etcd'

    -- Setup storage.
    before_all_default(g, {storage = storage})

    -- Create a client.
    g.client = conf_lib.new(g.client_urls, {
        driver = storage,
    })
end)

g.after_all(function()
    -- Teardown storage.
    after_all_default(g)

    -- Drop the client.
    g.client = nil
end)

-- }}} Setup / teardown

-- {{{ Basic flow

g.test_basic_flow = function()
    -- XXX: Use key / value generators or at least generated
    -- prefix to make the test immutable for repeated / parallel
    -- runs.

    -- Scalar.
    g.client:set('foo', 42)
    local res = g.client:get('foo')
    t.assert_equals(res.data, 42)

    -- Delete scalar.
    g.client:del('foo')
    local res = g.client:get('foo')
    t.assert_equals(res.data, nil)

    -- Set nested scalar.
    g.client:set('foo.bar', 42)
    local res = g.client:get('foo.bar')
    t.assert_equals(res.data, 42)
    local res = g.client:get('foo')
    t.assert_equals(res.data, {bar = 42})

    -- Set map.
    g.client:set('foo', {bar = {baz = 42}})
    local res = g.client:get('foo')
    t.assert_equals(res.data, {bar = {baz = 42}})
    local res = g.client:get('foo.bar')
    t.assert_equals(res.data, {baz = 42})
    local res = g.client:get('foo.bar.baz')
    t.assert_equals(res.data, 42)

    -- Set a field, delete a field.
    g.client:set('foo.x', 6)
    g.client:del('foo.bar')
    local res = g.client:get('foo')
    t.assert_equals(res.data, {x = 6})

    -- Set an array.
    g.client:set('foo', {'a', 'b', 'c'})
    local res = g.client:get('foo')
    t.assert_equals(res.data, {'a', 'b', 'c'})
    local res = g.client:get('foo.1')
    t.assert_equals(res.data, 'a')
    local res = g.client:get('foo.2')
    t.assert_equals(res.data, 'b')
    local res = g.client:get('foo.3')
    t.assert_equals(res.data, 'c')

    -- Clean up.
    g.client:del('foo')
end

-- }}} Basic flow

-- {{{ .new() parameters validation

g.test_new_params_validation = function()
    local opts = {driver = 'etcd'}
    t.assert_error_msg_content_equals(
        'endpoints is the mandatory parameter',
        conf_lib.new, nil, opts)
    t.assert_error_msg_content_equals(
        'endpoints parameter must be table, got string',
        conf_lib.new, g.etcd_client_urls[1], opts)
    t.assert_error_msg_content_equals(
        'endpoints parameter must not be empty',
        conf_lib.new, {}, opts)
end

-- }}} .new() parameters validation
