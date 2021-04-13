-- Adaptor for the etcd client to the conf.client API.
--
-- A driver API is the same as conf.client API.

local json = require('json')
local scalar_serializer = require('conf.client.etcd.scalar_serializer')
local etcd_client = require('conf.client.etcd')

-- Forward declaration.
local mt

-- This marker cannot be confused with a scalar value: there is no
-- !!table tag in the scalar serializer. Moreover, without a
-- whitespace afterwards the scalar serializer would interperet it
-- as ill-formed tagged value.
local TABLE_MARKER = '!!table'

-- {{{ Helpers

-- Verify that a parent key does not hold a scalar.
local function assert_parent_is_a_table(self, key)
    if not key:find('%.') then
        -- 'key' has no parent key, so nothing to check.
        return
    end

    local client = rawget(self, 'client')
    local parent_key = key:gsub('%.[^.]+$', '')
    local response = client:range(parent_key)
    assert(response.count <= 1)

    -- No value.
    if response.count == 0 then
        -- TODO: Raise some kind of TypeError (see below).
        error(('Attempt to access "%s", but there is no "%s"'):format(
            key, parent_key))
    end

    -- Scalar.
    if response.kvs[1].value ~= TABLE_MARKER then
        -- TODO: It would be more appropriate to raise an
        -- error of a suitable type: a kind of Python's
        -- TypeError. We raise a plain string error only
        -- on arguments validation.
        error(('Attempt to access "%s", but "%s" is a scalar'):format(
            key, parent_key))
    end
end

-- }}} Helpers

-- {{{ Flatten / unflatten

local flatten_impl
flatten_impl = function(basepath, obj, kvs)
    -- Scalar.
    if type(obj) ~= 'table' then
        table.insert(kvs, {
            key = basepath,
            value = scalar_serializer.encode(obj),
        })
        return
    end

    -- Array or map.
    table.insert(kvs, {
        key = basepath,
        value = TABLE_MARKER,
    })
    for k, v in pairs(obj) do
        -- XXX: Validate key:
        --
        -- * there should be no '.'
        -- * forbid strings like '1'
        flatten_impl(('%s.%s'):format(basepath, tostring(k)), v, kvs)
    end
end

-- Example:
--
--  | 'foo', {bar = 6, baz = 7}
--  |
--  | =>
--  |
--  | {
--  |     {key = 'foo', value = '!!table'},
--  |     {key = 'foo.bar', value = '6'},
--  |     {key = 'foo.baz', value = '7'},
--  | }
--
-- Example with an array:
--
--  | 'foo', {'a', 'b', 'c'}
--  |
--  | =>
--  |
--  | {
--  |     {key = 'foo', value = '!!table'},
--  |     {key = 'foo.1', value = 'a'},
--  |     {key = 'foo.2', value = 'b'},
--  |     {key = 'foo.3', value = 'c'},
--  | }
local function flatten(basepath, obj)
    -- XXX: Rewrite as an iterator?
    local kvs = {}
    flatten_impl(basepath, obj, kvs)
    return kvs
end

local function unflatten_error(basepath, kvs, key)
    -- TODO: Raise a kind of TypeError.
    error(('unflatten: the data received for "%s" look inconsistent or ' ..
        'wrongly sorted (error on key "%s"): %s'):format(basepath, key,
        json.encode(kvs)))
end

-- Example 1:
--
--  | 'foo', {
--  |     {key = 'foo.bar', value = '42'},
--  | }
--  |
--  | =>
--  |
--  | {bar = 42}
--
-- Example 2:
--
--  | 'foo', {
--  |     {key = 'foo.bar', value = '!!table'},
--  |     {key = 'foo.bar.baz', value = '42'},
--  | }
--  |
--  | =>
--  |
--  | {bar = {baz = 42}}
--
-- Example 3:
--
--  | 'foo', {
--  |     {key = 'foo.bar', value = '!!table'},
--  |     {key = 'foo.bar.baz', value = '!!table'},
--  |     {key = 'foo.bar.baz.fiz', value = '42'},
--  | }
--  |
--  | =>
--  |
--  | {bar = baz = {fiz = 42}}
--
-- The order is important: parents should go first.
local function unflatten(basepath, kvs)
    local obj = {}

    for _, kv in ipairs(kvs) do
        -- Extract a relative path.
        assert(kv.key:startswith(basepath .. '.'))
        local relpath = kv.key:sub(#basepath + 2):split('.')

        -- Pass over path and advance cur_obj to assign its field.
        local cur_obj = obj
        for i = 1, #relpath - 1 do
            local component = relpath[i]
            component = tonumber(component) or component
            if type(cur_obj) ~= 'table' then
                unflatten_error(basepath, kvs, kv.key)
            end
            cur_obj = cur_obj[component]
        end

        -- Decode and assign kv.value.
        local component = relpath[#relpath]
        component = tonumber(component) or component
        if cur_obj[component] ~= nil then
            unflatten_error(basepath, kvs, kv.key)
        end
        cur_obj[component] = kv.value == TABLE_MARKER and {} or
            scalar_serializer.decode(kv.value)
    end

    return obj
end

-- }}} Flatten / unflatten

-- {{{ Module functions

local function new(endpoints, opts)
    return setmetatable({
        client = etcd_client.new(endpoints, opts),
    }, mt)
end

-- }}} Module functions

-- {{{ Instance methods

local function get(self, key)
    -- XXX: Make it transactional.
    local client = rawget(self, 'client')

    -- XXX: Should we check it at reading?
    assert_parent_is_a_table(self, key)

    local response_point = client:range(key)
    assert(response_point.count <= 1)

    -- No value.
    if response_point.count == 0 then
        return {} -- data == nil
    end

    -- Scalar.
    if response_point.kvs[1].value ~= TABLE_MARKER then
        local kv = response_point.kvs[1]
        return {
            data = scalar_serializer.decode(kv.value)
        }
    end

    -- Table.
    --
    -- The order is important: see unflatten() comment.
    local response_range = client:range(key .. '.', client.NEXT, {
        sort_order = 'ASCEND',
        sort_target = 'KEY',
    })
    local obj = unflatten(key, response_range.kvs)
    return {
        data = obj,
    }
end

local function set(self, key, obj)
    -- XXX: Make it transactional.
    local client = rawget(self, 'client')
    assert_parent_is_a_table(self, key)
    client:deleterange(key)
    client:deleterange(key .. '.', client.NEXT)
    for _, kv in ipairs(flatten(key, obj)) do
        client:put(kv.key, kv.value)
    end
end

local function del(self, key)
    -- XXX: Make it transactional.
    local client = rawget(self, 'client')

    -- XXX: Should we check it before deletion?
    assert_parent_is_a_table(self, key)

    client:deleterange(key)
    client:deleterange(key .. '.', client.NEXT)
end

mt = {
    __index = {
        get = get,
        set = set,
        del = del,
    }
}

-- }}} Instance methods

return {
    new = new,
}
