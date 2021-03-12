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
    g.client = conf_lib.new({
        driver = storage,
        endpoints = g.client_urls,
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
    g.client:set('foo', {})
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
    t.assert_error_msg_content_equals(
        'endpoints is the mandatory parameter',
        conf_lib.new, {driver = 'etcd'})
    t.assert_error_msg_content_equals(
        'endpoints parameter must be table, got string',
        conf_lib.new, {driver = 'etcd', endpoints = g.etcd_client_urls[1]})
    t.assert_error_msg_content_equals(
        'endpoints parameter must not be empty',
        conf_lib.new, {driver = 'etcd', endpoints = {}})
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

    -- Set a scalar 'inside' an existing scalar.
    --
    -- It is forbidden.
    local child_key = ('%s.%s'):format(key, gen_key())
    local exp_err = ('Attempt to access "%s", but "%s" is a scalar'):format(
        child_key, key)
    local ok, err = pcall(g.client.set, g.client, child_key, gen_value())
    check(ok, err, exp_err)

    -- Get returns nil for the 'inside scalar' key.
    --
    -- The logic is the following: we check parent's data type
    -- only when operation may leave the data in an inconsistent
    -- state (a value under scalar, a value under a non-existent
    -- value).
    --
    -- So get() just tries to fetch the requested key and show
    -- that there is no data under it.
    local res = g.client:get(child_key)
    t.assert_equals(res.data, nil)

    -- Delete does not check whether a value exists and whether
    -- its parent is a table. The logic is described above.
    --
    -- del() just removes the data if there is something to
    -- remove.
    local ok = pcall(g.client.del, g.client, child_key)
    t.assert(ok)

    -- Attempt to set a value 'inside' a non-existent value
    -- that is under a scalar.
    --
    -- Since set() checks only parent's type, it see that there
    -- is no parent key and reports this error.
    local descendant_key = ('%s.%s.%s'):format(key, gen_key(), gen_key())
    local exp_err = ('Attempt to access "%s", but there is no "%s"'):format(
        descendant_key, descendant_key:gsub('%.[^.]+$', ''))
    local ok, err = pcall(g.client.set, g.client, descendant_key, gen_value())
    check(ok, err, exp_err)

    -- Everything is the same for get/del as in the cases above.
    local res = g.client:get(descendant_key)
    t.assert_equals(res.data, nil)
    local ok = pcall(g.client.del, g.client, descendant_key)
    t.assert(ok)
end

-- }}} Attempt to access a field of a scalar

-- {{{ Access a field / an item of a non-existing value

g.test_index_non_existing_value = function()
    -- set() fails on attempt to put something into a non-existent
    -- key.
    --
    -- get() and del() succeed.
    --
    -- See explanations in the
    -- test_access_descendant_of_a_scalar() test case.

    local function check(ok, err, exp_err)
        local err_msg = tostring(err):gsub('^.-:.-: ', '')
        t.assert(not ok)
        t.assert_equals(err_msg, exp_err)
    end

    local key_1 = gen_key()
    local key_2 = gen_key()
    local key = ('%s.%s'):format(key_1, key_2)
    local exp_err = ('Attempt to access "%s.%s", but there is no "%s"'):format(
        key_1, key_2, key_1)

    local ok, err = pcall(g.client.set, g.client, key, gen_value())
    check(ok, err, exp_err)

    local res = g.client:get(key, gen_value())
    t.assert_equals(res.data, nil)

    local ok = pcall(g.client.del, g.client, key, gen_value())
    t.assert(ok)
end

-- }}} Access a field / an item of a non-existing value

-- {{{ Don't confuse an empty table with lack of a value

g.test_marshalling_empty_table = function()
    local key = gen_key()
    g.client:set(key, {})

    local res = g.client:get(key)
    t.assert_equals(res.data, {})
end

g.test_remove_last_map_field = function()
    local key_1 = gen_key()
    local key_2 = gen_key()
    local key_3 = gen_key()
    local value = gen_value()

    g.client:set(key_1, {[key_2] = {[key_3] = value}})
    g.client:del(('%s.%s.%s'):format(key_1, key_2, key_3))

    local res = g.client:get(key_1)
    t.assert_equals(res.data, {[key_2] = {}})
end

-- }}} Don't confuse an empty table with lack of a value
