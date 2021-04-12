-- Adaptor for the etcd client to the conf.client API.
--
-- A driver API is the same as conf.client API.

local scalar_serializer = require('conf.client.etcd.scalar_serializer')
local etcd_client = require('conf.client.etcd')

-- Forward declaration.
local mt

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

    -- Presence of a value means that it is a scalar.
    --
    -- Tables are not tracked explicitly now.
    if response.count > 0 then
        -- TODO: It would be more appropriate to raise an
        -- error of a suitable type: a kind of Python's
        -- TypeError. We raise a plain string error only
        -- on arguments validation.
        error(('Attempt to access a field / an item "%s" of a scalar ' ..
            'value "%s"'):format(key, parent_key))
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
--  |     {key = 'foo.bar', value = 6},
--  |     {key = 'foo.baz', value = 7},
--  | }
--
-- Example with an array:
--
--  | 'foo', {'a', 'b', 'c'}
--  |
--  | =>
--  |
--  | {
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

-- Example 1:
--
--  | 'foo', {
--  |     {key = 'foo', value = 42},
--  | }
--  |
--  | =>
--  |
--  | 42
--
-- Example 2:
--
--  | 'foo', {
--  |     {key = 'foo.bar', value = 42},
--  | }
--  |
--  | =>
--  |
--  | {bar = 42}
local function unflatten(basepath, kvs)
    local obj
    for _, kv in ipairs(kvs) do
        local abspath = kv.key
        if abspath == basepath then
            assert(obj == nil)
            obj = scalar_serializer.decode(kv.value)
        else
            assert(obj == nil or type(obj) == 'table')
            if obj == nil then
                obj = {}
            end
            assert(kv.key:startswith(basepath .. '.'))
            local relpath = kv.key:sub(#basepath + 2):split('.')
            local cur_obj = obj
            for i = 1, #relpath - 1 do
                local component = relpath[i]
                component = tonumber(component) or component
                assert(cur_obj[component] == nil or
                    type(cur_obj[component]) == 'table')
                if cur_obj[component] == nil then
                    cur_obj[component] = {}
                end
                cur_obj = cur_obj[component]
            end
            local component = relpath[#relpath]
            component = tonumber(component) or component
            assert(cur_obj[component] == nil)
            cur_obj[component] = scalar_serializer.decode(kv.value)
        end
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
    assert_parent_is_a_table(self, key)
    local response_point = client:range(key)
    local response_range = client:range(key .. '.', client.NEXT)
    if response_point.count > 0 and response_range.count > 0 then
        -- TODO: Raise an error of a suitable type: a kind of
        -- Python's TypeError. We raise a plain string error only
        -- on arguments validation.
        error(('Data in the storage look corrupted: the key "%s" holds ' ..
            'a scalar, however it has descendant keys'):format(key))
    end
    local response = response_point.count > 0 and response_point or
        response_range
    local obj = unflatten(key, response.kvs)
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
