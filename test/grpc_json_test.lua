local digest = require('digest')
local t = require('luatest')
local grpc_json = require('conf.driver.etcd.grpc_json')

local g = t.group()

-- Accepts a table.
local copy_deep
copy_deep = function(src)
    local res = {}
    for k, v in pairs(src) do
        if type(v) == 'table' then
            v = copy_deep(v)
        end
        res[k] = v
    end
    return res
end

local protocol = grpc_json.new_protocol()
protocol:add_enum('MyEnum', {
    [0] = 'FOO',
    [1] = 'BAR',
})
protocol:add_message('MyMessage', {
    [1] = {'bytes', 'bytes_field'},
    [2] = {'bool', 'bool_field'},
    [3] = {'int64', 'int64_field'},
    [4] = {'uint64', 'uint64_field'},
    [5] = {'MyEnum', 'enum_field'},
    [6] = {'repeated', 'int64', 'int64_array'}
})
protocol:add_message('ComplexMessage', {
    [1] = {'MyMessage', 'my_message_field'},
    [2] = {'repeated', 'MyMessage', 'my_message_array'},
})

g.test_success_encode_decode = function()
    local my_message_default = {
        bytes_field = '',
        bool_field = false,
        int64_field = 0,
        uint64_field = 0,
        enum_field = 'FOO',
        int64_array = {},
    }
    local encode_src = {
        my_message_field = {
            bytes_field = 'foo',
            bool_field = true,
            int64_field = -42,
            uint64_field = 42,
            enum_field = 'BAR',
            int64_array = {1, 2, 3},
        },
        my_message_array = {
            {bool_field = true},
            {int64_field = 8},
        },
    }

    -- Bytes are encoded with base64, omitted fields are left
    -- omitted.
    local encode_exp = copy_deep(encode_src)
    encode_exp.my_message_field.bytes_field =
        digest.base64_encode(encode_src.my_message_field.bytes_field)
    local res = protocol:encode('ComplexMessage', encode_src)
    t.assert_equals(res, encode_exp, 'verify encode')

    -- Bytes are decoded from base64, omitted fields are set to
    -- default values.
    local decode_src = encode_exp
    local decode_exp = encode_src
    decode_exp.my_message_array[1] = table.copy(my_message_default)
    decode_exp.my_message_array[1].bool_field = true
    decode_exp.my_message_array[2] = table.copy(my_message_default)
    decode_exp.my_message_array[2].int64_field = 8

    local res = protocol:decode('ComplexMessage', decode_src)
    t.assert_equals(res, decode_exp, 'verify decode')
end

g.test_scalar_validation = function()
    local cases = {
        {
            typename = 'MyMessage',
            src = {bytes_field = 42},
            exp_err = '{MyMessage} bytes_field: Expected string, got number',
        },
        {
            typename = 'MyMessage',
            src = {int64_field = 2^54},
            exp_err = '{MyMessage} int64_field: int64 value should be < 2^53',
        },
        {
            typename = 'MyMessage',
            src = {enum_field = true},
            exp_err = '{MyMessage} enum_field: Non-string value (boolean) ' ..
                'given for enum "MyEnum"',
        },
        {
            typename = 'MyMessage',
            src = {enum_field = 'UNKNOWN'},
            exp_err = '{MyMessage} enum_field: Unknown value "UNKNOWN" in ' ..
                'enum "MyEnum"',
        },
        {
            typename = 'MyMessage',
            src = {unknown_field = 'foo'},
            exp_err = '{MyMessage}: No field "unknown_field" in message ' ..
                '"MyMessage"',
        },
        {
            typename = 'ComplexMessage',
            src = {my_message_array = true},
            exp_err = '{ComplexMessage} my_message_array: A non-table value ' ..
                '(boolean) is given for array of "MyMessage"',
        },
        {
            typename = 'ComplexMessage',
            src = {my_message_field = true},
            exp_err = '{ComplexMessage} my_message_field: A non-table value ' ..
                '(boolean) is given for message "MyMessage"',
        },
        {
            typename = 'ComplexMessage',
            src = {my_message_array = {{bool_field = 55}}},
            exp_err = '{ComplexMessage} my_message_array[1].bool_field: ' ..
                'Expected boolean, got number',
        },
        {
            typename = 'ComplexMessage',
            src = {my_message_field = {bool_field = 55}},
            exp_err = '{ComplexMessage} my_message_field.bool_field: ' ..
                'Expected boolean, got number',
        },
    }
    for _, case in ipairs(cases) do
        t.assert_error_msg_content_equals(case.exp_err, protocol.encode,
            protocol, case.typename, case.src)
    end
end
