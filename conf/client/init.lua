--- Configuration storage client.
--
-- @module conf

local etcd_driver = require('conf.client.etcd.driver')

-- Forward declaration.
local mt

local supported_drivers = {
    ['etcd'] = etcd_driver,
}

-- {{{ Module functions

--- Module functions.
--
-- @section Functions

--- Create a new configuration storage client instance.
--
-- @array[string] endpoints
--     Endpoint URLs.
-- @table         opts
--     Client options.
--
--     May contain driver specific options (listed in
--     @{conf.client.etcd.new}).
-- @string        opts.driver
--     Driver name. Only 'etcd' is supported now.
--
--     It is the mandatory option.
--
-- @raise See 'General API notes' in the @{conf.client.etcd|etcd
-- client documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd client documentation.
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
        error('driver is the mandatory option')
    end
    if type(driver) == 'string' then
        driver = supported_drivers[driver]
        if not driver then
            error(('Unknown driver: %s'):format(driver))
        end
    else
        -- TODO: Allow to pass an external driver here (a module
        -- table). However we should perform some API checks in
        -- the case: check that driver.new is a callable, check
        -- that instance methods are callables (after creating an
        -- instance).
        --
        -- Without proper validation it is quite easy to receive
        -- non quite informative errors like 'attempt to call a
        -- nil value'.
        error('driver is not a string: only built-in drivers are supported now')
    end
    return setmetatable({
        driver = driver.new(endpoints, opts),
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
-- @raise See 'General API notes' in the @{conf.client.etcd|etcd
-- client documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd client documentation.
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
    return rawget(self, 'driver'):get(key)
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
-- @raise See 'General API notes' in the @{conf.client.etcd|etcd
-- client documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd client documentation.
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
    rawget(self, 'driver'):set(key, obj)
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
-- @raise See 'General API notes' in the @{conf.client.etcd|etcd
-- client documentation}.
--
-- XXX: Move the General API notes (at least errors) outside of
-- the etcd client documentation.
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
    rawget(self, 'driver'):del(key)
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
