--- etcd driver.
-- @module conf.driver.etcd

local utils = require('conf.driver.etcd.utils')
local protocol = require('conf.driver.etcd.protocol')
local transport = require('conf.driver.etcd.transport')

-- Forward declaration.
local mt

-- {{{ Module / instance constants

-- TODO: Add NEXT / ALL to docs.

local function next_key(key)
    -- TODO: '' and '\xff\xff' gives '\0' == ALL, is it correct?
    local len = string.len(key)
    local bytes = {string.byte(key, 1, len)}
    for i = len, 1, -1 do
        if bytes[i] ~= 0xff then
            bytes[i] = bytes[i] + 1
            break
        end
        len = len - 1
    end
    return len == 0 and '\0' or string.char(unpack(bytes, 1, len))
end

local NEXT = next_key
local ALL = '\0'

-- }}} Module / instance constants

-- {{{ Module functions

--- Module functions.
--
-- @section Functions

--- Create a new etcd client instance.
--
-- If etcd is [started][1] with `--auto-tls` (or there is other
-- reason to don't verify server's identity on the client), set
-- the `opts.http_client.request.verify_peer` option to `false`.
-- It corresponds to the `--insecure` (`-k`) curl option.
--
-- If the etcd server is [secured][1] by enforcement of clients to
-- use a client certificate, use `opts.http_client.request.ssl_key`
-- and `opts.http_client.request.ssl_cert` options. They
-- correspond to the `--key` and `--cert` curl options.
--
-- XXX: Add those options to the root level?
--
-- Please, note that this etcd client cannot use TLS Common Name
-- to authenticate with the server, because the gRPC to JSON
-- gateway does not support it.
--
-- If the [authentication][2] is enabled on the etcd server,
-- provide `opts.user` and `opts.password` options.
--
-- etcd may be configured to don't require neither client side
-- key & certificate, nor user ID & password.
--
-- [1]: https://etcd.io/docs/v3.4.0/op-guide/security/
-- [2]: https://etcd.io/docs/v3.4.0/learning/design-auth-v3/
--
-- @table[opt]   opts
--     etcd client options.
-- @array[string] opts.endpoints
--     Endpoint URLs.
-- @string[opt]  opts.user
--     A user ID to authenticate with the server.
-- @string[opt]  opts.password
--     A password to authenticate with given User ID.
-- @table[opt]   opts.http_client
--     HTTP client options.
-- @table[opt]   opts.http_client.new
--     @{etcd.transport.http_client_new_opts|HTTP client instance
--     options}.
-- @table[opt]   opts.http_client.request
--     @{etcd.transport.http_client_request_opts|HTTP client
--     request options}.
--
-- @raise See 'General API notes'.
--
-- @return etcd client instance.
--
-- @function conf.driver.etcd.new
local function new(opts)
    -- XXX: Handle user & password: give transport a callback
    -- to obtain authorization header.

    -- The protocol is stored to give a user ability to extend it
    -- with some definitions that are not supported by the client
    -- itself.
    return setmetatable({
        protocol = protocol.new(),
        transport = transport.new({
            endpoints = opts.endpoints,
            http_client = opts.http_client,
        }),
    }, mt)
end

-- }}} Module functions

-- {{{ Instance methods

--- Instance methods.
--
-- @section Methods

--- Put the given key into the key-value storage.
--
-- Creates a key if it does not exist. Increments a revision of
-- the key-value store. Generates one event in the event history.
--
-- @param self
--     etcd driver instance.
-- @string key
--     Key to put.
-- @string value
--     Value to associate with the key.
--
--     Use opts.ignore_value to leave the existing value
--     unchanged.
--
--     Technically it is possible to omit the value without the
--     opts.ignore_value option, however the result may be
--     misleading. The default string value (an empty string)
--     will be associated with the key.
--
--     TODO: Consider dropping the ignore_value option from the
--     API and provide this behavious, when value == nil is
--     given. In Lua, unlike protobuf v3, we able to distinguish
--     nil and an enpty string.
-- @table[opt] opts
--     Put request options.
-- @integer[opt] opts.lease
--     Lease ID to associate with the key. Zero indicates no
--     lease.
--
--     TODO: When omitted -- use current lease or drops the lease
--     on the key? Test it and update the doc.
--
--     TODO: Describe how to obtain a lease ID.
--
--     Note: Use 'ignore_lease' to reuse currently existing lease.
-- @boolean[opt] opts.prev_kv
--     Whether to return the previous key-value pair in the
--     response.
-- @boolean[opt] opts.ignore_value
--     Update the key using the **existing** value: so 'value'
--     remains the same, but 'mod_revision' and 'version' are
--     updated.
--
--     Returns the INVALID_ARGUMENT error if the key does not
--     exist.
-- @boolean[opt] opts.ignore_lease
--     Update the key using the **existing** lease.
--
--     Returns the INVALID_ARGUMENT error if the key does not
--     exist.
--
-- @raise See 'General API notes'.
--
-- @return Response of the following structure:
--
-- ```
-- {
--     header = ResponseHeader,
--     prev_kv = KeyValue (if prev_kv is set),
-- }
-- ```
--
-- @see ResponseHeader
-- @see KeyValue
--
-- @function instance.put
local function put(self, key, value, opts)
    local protocol = rawget(self, 'protocol')
    local opts = opts or {}
    local request = protocol:encode('PutRequest', utils.merge_deep({
        key = key,
        value = value,
    }, opts))
    local response = rawget(self, 'transport'):request('/v3/kv/put', request)
    return protocol:decode('PutResponse', response)
end

--- Fetch a range of keys and its values.
--
-- This methods allows to fetch key-value pairs by a key,
-- a prefix of a key, a range of keys.
--
-- The following table summarizes different ways to indicate
-- a range of keys.
--
-- | key   | range_end | effect                     |
-- | ----- | --------- | -------------------------- |
-- | 'foo' | nil       | keys exactly matched 'foo' |
-- | 'foo' | 'zoo'     | keys in ['foo', 'zoo')     |
-- | 'foo' | NEXT      | keys prefixed with 'foo'   |
-- | 'foo' | ALL       | keys >= 'foo'              |
-- | ALL   | 'foo'     | keys < 'foo'               |
-- | ALL   | ALL       | all keys                   |
--
-- @param self
--     etcd driver instance.
-- @string key
--     A key, a key prefix or a range start (depending of
--     'range_end' value).
--
--     The special value `conf.driver.etcd.ALL` means no lower
--     boundary.
-- @string range_end
--     Upper boundary of the range (exclusive).
--
--     The special value `conf.driver.etcd.NEXT` means to fetch
--     all values prefixed with 'key'.
--
--     The special value `conf.driver.etcd.ALL` means no upper
--     boundary.
--
--     A function (key -> range_end) may be passed here.
-- @string opts
--     Range request options.
-- @integer opts.limit
--     Limit number of returned keys. Zero means no limit.
-- @integer opts.revision
--     Revision (a kind of point-in-time) to use for the request.
--     Negative or zero value means using of the newest revision.
--
--     Note: The response header always contains the newest
--     revision. The option affects returned key-values.
--
--     Returns the OUT_OF_RANGE error if the revision is compacted.
-- @string opts.sort_order
--     Order for returned result:
--
--     - 'NONE' (default, no sorting),
--     - 'ASCEND' (lowest target value first),
--     - 'DESCEND' (highest target value first).
-- @string opts.sort_target
--     KeyValue field to use for sorting: 'KEY' (default), 'VERSION',
--     'CREATE', 'MOD', 'VALUE'.
-- @boolean opts.serializable
--     Use serializable member-local reads.
--
--     Range requests are linearizable by default; linearizable
--     requests have higher latency and lower throughput than
--     serializable requests but reflect the current consensus of
--     the cluster.
--
--     For better performance, in exchange for possible stale
--     reads, a serializable range request is served locally
--     without needing to reach consensus with other nodes in the
--     cluster.
-- @boolean opts.keys_only
--     Return only keys, not values.
--
--     Since all omitted fields are set to default values (which
--     are defined by protobuf v3), all fetched values will appear
--     as empty strings in the response.
-- @boolean opts.count_only
--     Return only the keys count in the range, but not key-values
--     itself (so, `response.kvs` will be an empty table).
--
--     `response.count` is set always, disregarding of the
--     `opts.count_only` value in the request.
-- @integer opts.min_mod_revision
--     Filter out keys with `mod_revision` lesser than given
--     revision.
-- @integer opts.max_mod_revision
--     Filter out keys with `mod_revision` greater than given
--     revision.
-- @integer opts.min_create_revision
--     Filter out keys with `create_revision` lesser than given
--     revision.
-- @integer opts.max_create_resivion
--     Filter out keys with `create_revision` greater than given
--     revision.
--
-- @raise See 'General API notes'.
--
-- @return Response of the following structure:
--
-- ```
-- {
--     header = ResponseHeader,
--     kvs = array of KeyValue (empty table when opts.count_only is true),
--     more = boolean (if there are more keys to return in the
--         requested range),
--     count = integer,
-- }
-- ```
--
-- @see ResponseHeader
-- @see KeyValue
--
-- @function instance.range
local function range(self, key, range_end, opts)
    local protocol = rawget(self, 'protocol')
    local opts = opts or {}
    if type(range_end) == 'function' then
        range_end = range_end(key)
    end
    local request = protocol:encode('RangeRequest', utils.merge_deep({
        key = key,
        range_end = range_end,
    }, opts))
    local response = rawget(self, 'transport'):request('/v3/kv/range', request)
    return protocol:decode('RangeResponse', response)
end

mt = {
    __index = {
        put = put,
        range = range,
        NEXT = NEXT,
        ALL = ALL,
    }
}

-- }}} Instance methods

-- {{{ General API notes

--- General API notes.
--
-- Integers.
--
-- Whenever a request or response field is marked as `integer`,
-- the API accepts and may return two types: `number` or so
-- called `number64` (e.g. 1LL or 1ULL). Signed and unsigned
-- integers are not marked explicitly. One may find precise types
-- in the protocol description: [here][1] and [here][2].
--
-- [1]: https://github.com/etcd-io/etcd/blob/v3.4.15/mvcc/mvccpb/kv.proto
-- [2]: https://github.com/etcd-io/etcd/blob/v3.4.15/etcdserver/etcdserverpb/rpc.proto
--
-- The subsections below describe responses parts, which are
-- common for different responses.
--
-- Errors.
--
-- Functions of the module may raise different kinds of errors:
--
-- 1. Arguments validation error (Lua string).
--
-- 2. @{EtcdError}.
--
-- 3. @{HttpError}.
--
-- 4. Internal HTTP client error: say, out-of-memory during the
--    request (`box.error` instance).
--
-- The subsections below describe content of those errors.
--
-- @section options

--- EtcdError
--
-- @anchor EtcdError
--
-- @integer    code
--     GRPC error code.
--
--     Those codes are listed in the `conf.driver.etcd.error`
--     module.
-- @string     message
--     What is going on wrong.
-- @param[opt] details
--     Extra data if etcd provides it.
-- @string     code_name
--     Human readable name of the code.

--- HttpError
--
-- @anchor HttpError
--
-- @param response
--     Raw HTTP response.
-- @integer     response.status
--     HTTP code.
-- @string response.reason
--     Human readable interpretation of the code. Not very useful
--     at the moment of writting ('Ok' or 'Unknown').
-- @table[opt] response.headers
--     HTTP headers.
-- @string[opt] response.body
--     HTTP response body.

--- ResponseHeader
--
-- @anchor ResponseHeader
--
-- @integer cluster_id
--     ID of the cluster, which sent the response.
-- @integer member_id
--     ID of the member, which sent the response.
-- @integer revision
--     Storage revision, which is generated when the request was
--     applied.
--
--     TODO: Clarify when the revision is guaranteed to grow
--     monotonically with modifications. I guess it is so for any
--     linearizable requests (when opts.serializable is not set
--     for a range request) or when we're sure there were no
--     reconnect to another cluster node.
-- @integer raft_term
--     Raft term, when the request was applied.

--- KeyValue
--
-- @anchor KeyValue
--
-- @string key
--     String representing the key. An empty string can not be
--     used as a key.
-- @integer create_revision
--     Revision of last creation of this key.
-- @integer mod_revision
--     Revision of last modification of this key.
-- @integer version
--     Version of the key. A deletion resets it to zero, any
--     modification increases it.
-- @string value
--     Value associated with the key.
-- @integer lease
--     Lease ID attached to the key. Zero indicates no lease.
--
--     When the attached lease expires, the key will be deleted.

-- }}} General API notes

return {
    new = new,
    NEXT = NEXT,
    ALL = ALL,
}
