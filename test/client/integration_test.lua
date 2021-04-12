local conf_lib = require('conf')
local t = require('luatest')
local test_utils = require('test.utils')

local g = t.group()

-- Shortcuts.
local gen_key = test_utils.gen_key
local gen_value = test_utils.gen_value
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

-- {{{ Non-string scalars

g.test_number = function()
    local key = gen_key()

    -- Positive value.
    local value = 42
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, value)

    -- Negative value.
    local value = -42
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, value)
end

g.test_number64 = function()
    local key = gen_key()

    -- 2^64 - 1.
    local value = 18446744073709551615ULL
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, value)

    -- A small value is returned as a usual number.
    local exp_value = 42
    local value = 42LL
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, exp_value)
end

g.test_boolean = function()
    local key = gen_key()

    local value = false
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, value)

    local value = true
    g.client:set(key, value)
    local res = g.client:get(key)
    t.assert_equals(res.data, value)
end

-- }}} Non-string scalars

-- {{{ Holey array

g.test_set_holey_array = function()
    local key = gen_key()
    local arr = {gen_value(), nil, gen_value()}
    g.client:set(key, arr)

    local res = g.client:get(key)
    t.assert_equals(res.data, arr)
end

g.test_puncture_array = function()
    local key = gen_key()
    local arr = {gen_value(), gen_value(), gen_value()}
    g.client:set(key, arr)

    g.client:del((key .. '.2'))

    local res = g.client:get(key)
    t.assert_equals(res.data, {arr[1], nil, arr[3]})
end

-- }}} Holey array

-- {{{ Attempt to access a field of a scalar

g.test_access_descendant_of_a_scalar = function()
    local function check(ok, err, exp_err)
        local err_msg = tostring(err):gsub('^.-:.-: ', '')
        t.assert(not ok)
        t.assert_equals(err_msg, exp_err)
    end

    -- Set a scalar.
    local key = gen_key()
    local value = gen_value()
    g.client:set(key, value)

    -- Get/set/del a scalar 'inside' an existing scalar.
    local child_key = ('%s.%s'):format(key, gen_key())
    local exp_err = ('Attempt to access a field / an item "%s" of a scalar ' ..
        'value "%s"'):format(child_key, key)
    local ok, err = pcall(g.client.get, g.client, child_key)
    check(ok, err, exp_err)
    local ok, err = pcall(g.client.set, g.client, child_key, gen_value())
    check(ok, err, exp_err)
    local ok, err = pcall(g.client.del, g.client, child_key)
    check(ok, err, exp_err)

    -- The following test cases fail now.
    --[[
    -- Get/set/del a scalar deeply 'inside' an existing scalar.
    local descendant_key = ('%s.%s.%s'):format(key, gen_key(), gen_key())
    local exp_err = ('Attempt to access a field / an item "%s" of a scalar ' ..
        'value "%s"'):format(descendant_key, key)
    local ok, err = pcall(g.client.get, g.client, descendant_key)
    check(ok, err, exp_err)
    local ok, err = pcall(g.client.set, g.client, descendant_key, gen_value())
    check(ok, err, exp_err)
    local ok, err = pcall(g.client.del, g.client, descandant_key)
    check(ok, err, exp_err)
    --]]
end

-- }}} Attempt to access a field of a scalar
