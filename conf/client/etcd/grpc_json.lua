local digest = require('digest')
local utils = require('conf.client.etcd.utils')

local array_mt = {__serialize = 'sequence'}
local map_mt = {__serialize = 'mapping'}
local empty_array = setmetatable({}, array_mt)
local empty_map = setmetatable({}, map_mt)

-- Note: int64 / uint64 are received as strings from etcd, but
-- may be passed as numbers.
local scalar_assertions = {
    string = utils.assert_string,
    bytes = utils.assert_string,
    bool = utils.assert_boolean,
    int64 = utils.assert_int64,
    uint64 = utils.assert_uint64,
}

-- Trick: those values are the same after applying scalar
-- transformation functions from:
--
-- - encode_opts.transforms
-- - decode_opts.transforms
--
-- So we don't split them to 'lua' defaults and 'wire/json'
-- defaults.
local scalar_default = {
    string = '',
    bytes = '',
    bool = false,
    int64 = 0,
    uint64 = 0,
}

local function transform_enter(ctx, name)
    table.insert(ctx.path, name)
end

local function transform_leave(ctx)
    table.remove(ctx.path)
end

local function transform_path(ctx)
    local res = ''
    for _, name in ipairs(ctx.path) do
        if type(name) == 'number' then
            res = res .. ('[%d]'):format(name)
        else
            res = res .. '.' .. name
        end
    end
    return res:sub(2)
end

local function transform_error_prefix(ctx)
    if next(ctx.path) == nil then
        return ('{%s}'):format(ctx.upmost_typename)
    end
    return ('{%s} %s'):format(ctx.upmost_typename, transform_path(ctx))
end

local function transform_error(ctx, message, ...)
    local error_prefix = transform_error_prefix(ctx)
    error(('%s: %s'):format(error_prefix, message:format(...)))
end

-- Deeply validate the 'value' against a message/enum/scalar and
-- transform scalars according to passed functions.
--
-- The main goal of this function is to implement base64 encoding
-- and decoding of 'bytes' fields. When opts.set_default is
-- passed, omitted fields are saturated with default values.
--
-- etcd does not serialize a message field, when it has a default
-- value, so we should saturate responses with them for a user
-- convenience.
--
-- We don't serialize a field, when it is not passed, however, if
-- a default value is given, it'll be serialized and sent.
--
-- Options:
--
-- opts.transforms ([typename] = <function> mapping, mandatory)
-- opts.set_default (boolean, default is false)
-- opts.validate_scalar (boolean, default is false)
local transform_scalars
transform_scalars = function(protocol, typename, value, opts, ctx)
    -- Scalar.
    local transform_f = opts.transforms[typename]
    if transform_f ~= nil then
        if type(value) == 'table' then
            transform_error(ctx, 'A table value is given for scalar "%s"',
                typename)
        end
        if value == nil then
            if opts.set_default then
                return scalar_default[typename]
            end
            return nil
        end
        if opts.validate_scalar then
            local error_prefix = transform_error_prefix(ctx)
            scalar_assertions[typename](value, error_prefix)
        end
        return transform_f(value)
    end

    local enum = rawget(protocol, 'enums')[typename]
    if enum ~= nil then
        if value == nil then
            return opts.set_default and enum[0] or nil
        end
        if type(value) ~= 'string' then
            transform_error(ctx, 'Non-string value (%s) given for enum "%s"',
                type(value), typename)
        end
        if not enum[value] then
            transform_error(ctx, 'Unknown value "%s" in enum "%s"', value,
                typename)
        end
        return value
    end

    -- Repeated.
    if typename:startswith('repeated ') then
        if value == nil then
            return opts.set_default and empty_array or nil
        end
        local item_type = typename:sub(10)
        if type(value) ~= 'table' then
            transform_error(ctx, 'A non-table value (%s) is given for ' ..
                'array of "%s"', type(value), item_type)
        end
        local result = {}
        for item_num, item_value in ipairs(value) do
            transform_enter(ctx, item_num)
            result[item_num] = transform_scalars(protocol, item_type,
                item_value, opts, ctx)
            transform_leave(ctx)
        end
        return result
    end

    local message = rawget(protocol, 'messages')[typename]
    if message == nil then
        transform_error(ctx, 'No such typename: "%s"', typename)
    end

    -- The rest is about processing a message.

    if value == nil then
        return nil
    end

    if type(value) ~= 'table' then
        transform_error(ctx, 'A non-table value (%s) is given for message "%s"',
            type(value), typename)
    end

    -- Pass over the 'value' to catch unknown fields.
    for field_name, _ in pairs(value) do
        local field_type = message[field_name]
        if field_type == nil then
            transform_error(ctx, 'No field "%s" in message "%s"', field_name,
                typename)
        end
    end

    -- Pass over schema to store all values (important when
    -- opts.set_default is true).
    local result = {}
    for field_name, field_type in pairs(message) do
        transform_enter(ctx, field_name)
        local field_value = value[field_name]
        result[field_name] = transform_scalars(protocol,
            field_type, field_value, opts, ctx)
        transform_leave(ctx)
    end
    if next(result) == nil then
        return empty_map
    end
    return result
end

local function identity(...)
    return ...
end

local function protocol_add_enum(self, typename, enum_schema)
    -- Transform the enum to the {value -> true} mapping.
    local enum = {}
    -- Note: enums start from zero.
    for i, value in pairs(enum_schema) do
        enum[i] = value
        enum[value] = true
    end
    rawget(self, 'enums')[typename] = enum
end

local function protocol_add_message(self, typename, message_schema)
    -- Transform the message to the {field_name -> field_type}
    -- mapping.
    --
    -- My apologizes: prefixing with 'repeated ' looks a bit weird
    -- on the first glance, but, to be honest, typename as a table
    -- like {repeated = <boolean>, type = <...>} looks too
    -- complex.
    local message = {}
    for _, field in ipairs(message_schema) do
        if field[1] == 'repeated' then
            local field_type = field[2]
            local field_name = field[3]
            message[field_name] = 'repeated ' .. field_type
        else
            local field_type = field[1]
            local field_name = field[2]
            message[field_name] = field_type
        end
    end
    rawget(self, 'messages')[typename] = message
end

-- https://developers.google.com/protocol-buffers/docs/proto3#json

-- Regarding base64 encoding: etcd ignores newlines (see [1] and
-- [2]) in incoming bytes fields, but there is no sense to
-- send extra newline bytes. So we pass the `nowrap` option.
--
-- etcd accepts standard and urlsafe alphabets both and encoding
-- with/without paddings both (see [3]). We use the same encoding
-- as etcd itself: standard alphabet, with paddings.
--
-- [1]: https://github.com/grpc-ecosystem/grpc-gateway/pull/565
-- [2]: https://golang.org/pkg/encoding/base64/#Encoding.Decode
-- [3]: https://developers.google.com/protocol-buffers/docs/proto3#json
local encode_opts = {
    transforms = {
        string = identity,
        bytes = function(src)
            return digest.base64_encode(src, {nowrap = true})
        end,
        bool = identity,
        int64 = identity,
        uint64 = identity,
    },
    set_default = false,
    validate_scalar = true,
}

local decode_opts = {
    transforms = {
        string = identity,
        bytes = digest.base64_decode,
        bool = identity,
        int64 = tonumber64,
        uint64 = tonumber64,
    },
    set_default = true,
    -- The validation would fail on int64 / uint64 encoded as
    -- strings. Anyway, we assume that etcd gives correct
    -- types.
    validate_scalar = false,
}

local function protocol_encode(self, typename, value)
    local ctx = {path = {}, upmost_typename = typename}
    return transform_scalars(self, typename, value, encode_opts, ctx)
end

local function protocol_decode(self, typename, value)
    local ctx = {path = {}, upmost_typename = typename}
    return transform_scalars(self, typename, value, decode_opts, ctx)
end

local protocol_mt = {
    __index = {
        add_enum = protocol_add_enum,
        add_message = protocol_add_message,
        encode = protocol_encode,
        decode = protocol_decode,
    },
}

local function new_protocol()
    return setmetatable({
        enums = {},
        messages = {},
    }, protocol_mt)
end

return {
    new_protocol = new_protocol,
}
