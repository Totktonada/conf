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

--- Create new configuration storage client instance.
--
-- @array[string] endpoints
--     Endpoint URLs.
-- @table         opts
--     Client options.
--
--     May contain driver specific options (listed in
--     @{conf.driver.etcd.new}).
-- @string        opts.driver
--     Driver name. Only 'etcd' is supported now.
--
--     It is the mandatory option.
--
-- @raise See 'General API notes' in the @{conf.driver.etcd|etcd
-- driver documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd driver documentation.
--
-- @return client instance.
--
-- @usage
--
-- local conf_lib = require('conf')
--
-- local urls = {
--     'http://localhost:2379',
--     'http://localhost:2381',
--     'http://localhost:2383',
-- }
-- local conf = conf.new(urls, {driver = 'etcd'})
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
-- Note: A string value that can be interpreted as a number will
-- be converted to a number. It is constraint of current
-- implementation and will be resolved in a future.
--
-- @param  self
--     Client instance.
-- @string key
--     A key to fetch.
--
--     A dot notation may be used to access a nested map field or
--     array element: `'foo.bar'` or `'foo.1'`. Array indices are
--     1-based, just as in Lua.
--
--     XXX: Describe the dot notition is the separate section
--     (like 'General API notes').
--
-- @raise See 'General API notes' in the @{conf.driver.etcd|etcd
-- driver documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd driver documentation.
--
-- @return Response of the following structure:
--
-- ```
-- {
--     data = obj,
-- }
-- ```
--
-- `obj` is the value associated with given key or `nil` if given
-- key does not exist.
--
-- @usage
--
-- -- Put a map.
-- conf:set('foo', {bar = {baz = 42}})
--
-- -- Fetch it back.
-- local res = conf:get('foo')
-- -- res.data is {bar = {baz = 42}}
--
-- -- Or fetch a field.
-- local res = conf:get('foo.bar')
-- -- res.data is {baz = 42}
-- local res = conf:get('foo.bar.baz')
-- -- res.data is 42
--
-- -- Put an array.
-- conf:set('foo', {'a', 'b', 'c'})
--
-- -- Fetch an array element.
-- local res = conf:get('foo.1')
-- -- res.data is 'a'
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
-- If given key contains a value, the value is **replaced** with
-- the new one. Maps are NOT merged.
--
-- Use dot notation to set a particular field of a map (or
-- particular element of an array).
--
-- Storing of `foo.bar.baz` means that `foo` and `foo.bar` will
-- store a map. The same for `foo.1`: `foo` will store an array.
--
-- XXX: Define behaviour for corner cases like storing `foo.bar`,
-- when `foo` exists and it is not a map.
--
-- @param  self
--     Client instance.
-- @string key
--     A key to set.
--
--     A dot notation may be used, see @{conf.get}.
-- @param  obj
--     A value to store.
--
--     Allowed types: `string`, `number`, `table` (a map or an
--     array).
--
-- @raise See 'General API notes' in the @{conf.driver.etcd|etcd
-- driver documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd driver documentation.
--
-- @return Nothing.
--
-- It is recommended to don't lean on the fact that the method
-- does not return anything, because the API can be extended with
-- a return value in a future.
--
-- @usage
--
-- -- Store a scalar, a map and an array.
-- conf:set('foo', 'hello')
-- conf:set('moo', {bar = {baz = 42}})
-- conf:set('xoo', {'a', 'b', 'c'})
--
-- -- Set a nested field and an array element.
-- conf:set('foo.bar.baz', 'hello')
-- conf:set('foo.1', 'hello')
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
-- XXX: Define behaviour for tricky cases like removing an array
-- element in a middle of the array.
--
-- @param  self
--     Client instance.
-- @string key
--     A key to delete.
--
--     A dot notation may be used, see @{conf.get}.
--
-- @raise See 'General API notes' in the @{conf.driver.etcd|etcd
-- driver documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd driver documentation.
--
-- @return Nothing.
--
-- It is recommended to don't lean on the fact that the method
-- does not return anything, because the API can be extended with
-- a return value in a future.
--
-- @usage
--
-- -- Store a map and delete it then.
-- conf:set('foo', {bar = {baz = 42}})
-- conf:del('foo')
--
-- -- Or delete just one field.
-- conf:set('foo', {bar = {baz = 42}})
-- conf:del('foo.bar.baz')
--
-- -- Or delete an array element.
-- conf:set('foo', {'a', 'b', 'c'})
-- conf:del('foo.3')
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
