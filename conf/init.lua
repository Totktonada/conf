--- Configuration storage client.
--
-- @module conf

local etcd_driver = require('conf.driver.etcd')

-- Forward declaration.
local mt

-- {{{ Flatten / unflatten

local flatten_impl
flatten_impl = function(basepath, obj, kvs)
    -- Scalar.
    if type(obj) ~= 'table' then
        -- TODO: Here we loss type information. Should we handle
        -- it somehow?
        table.insert(kvs, {
            key = basepath,
            value = tostring(obj),
        })
        return
    end

    -- Array or map.
    for k, v in pairs(obj) do
        -- XXX: Validate key: there should be no '.'.
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
            -- TODO: There is no type information, so interpret
            -- any string that looks like a number as a number.
            --
            -- XXX: Use tonumber64().
            obj = tonumber(kv.value) or kv.value
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
            -- TODO: Same here, no type information, so interpret
            -- a number like string as a number.
            --
            -- XXX: Use tonumber64().
            cur_obj[component] = tonumber(kv.value) or kv.value
        end
    end

    return obj
end

-- }}} Flatten / unflatten

-- {{{ Module functions

local supported_drivers = {
    ['etcd'] = true,
}

--- Module functions.
--
-- @section Functions

--- Cleate new instance.
--
-- XXX: Write docs.
--
-- @function conf.new
local function new(endpoints, opts)
    local opts = opts or {}
    local driver = opts.driver
    if driver == nil then
        error('opts.driver is mandatory option')
    end
    if not supported_drivers[driver] then
        error(('Unknown opts.driver: %s'):format(driver))
    end
    -- XXX: Filter values.
    return setmetatable({
        driver = etcd_driver.new(endpoints, opts),
    }, mt)
end

-- }}} Module functions

-- {{{ Instance methods

--- Instance methods.
--
-- @section Methods

--- Fetch a value.
--
-- XXX: Write docs.
--
-- @function instance.get
local function get(self, key)
    -- XXX: Make it transactional.
    local response_point = self.driver:range(key)
    local response_range = self.driver:range(key .. '.', self.driver.NEXT)
    if response_point.count > 0 and response_range.count > 0 then
        error('XXX')
    end
    local response = response_point.count > 0 and response_point or
        response_range
    local obj = unflatten(key, response.kvs)
    return {
        data = obj,
    }
end

--- Store a value.
--
-- XXX: Write docs.
--
-- @function instance.set
local function set(self, key, obj)
    -- XXX: Make it transactional.
    self.driver:deleterange(key)
    self.driver:deleterange(key .. '.', self.driver.NEXT)
    for _, kv in ipairs(flatten(key, obj)) do
        self.driver:put(kv.key, kv.value)
    end
end

--- Delete a value.
--
-- XXX: Write docs.
--
-- @function instance.del
local function del(self, key)
    -- XXX: Make it transactional.
    self.driver:deleterange(key)
    self.driver:deleterange(key .. '.', self.driver.NEXT)
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
    internal = {
        flatten = flatten,
        unflatten = unflatten,
    }
}
