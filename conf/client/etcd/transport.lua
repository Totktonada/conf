--- HTTP transport for etcd client.
-- @module conf.client.etcd.transport

-- TODO: Save cluster_id as first connect and validate it at
-- reconnects.

local log = require('log')
local json = require('json')
local http_client_lib = require('http.client')
local utils = require('conf.client.etcd.utils')
local etcd_error = require('conf.client.etcd.error')

-- Forward declarations.
local http_client_new_opts_default
local http_client_request_opts_default

-- {{{ HTTP options defaults

--- Defaults.
--
-- @section defaults

--- Default HTTP client instance options.
--
-- Just empty table at the moment.
http_client_new_opts_default = {}

--- Default HTTP client request options.
--
-- Enables `keepalive_*` options to enforce the HTTP client to
-- hold connections for a while in order to reuse them for next
-- requests.
http_client_request_opts_default = {
    keepalive_idle = 60,
    keepalive_interval = 60,
}

-- }}} HTTP options defaults

-- {{{ HTTP options documentation

--- HTTP options.
--
-- @section options

--- HTTP client instance options.
--
-- Available HTTP client parameters may vary depending on
-- tarantool version. Refer to the HTTP client [documentation][1]
-- for precise list of available options.
--
-- [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/http/
--
-- @anchor http_client_new_opts
-- @integer[opt] max_connections
--     Maximum number of entries in the connection cache.
-- @integer[opt] max_total_connections
--     Maximum number of active connections.
--
-- XXX: verify descriptions
-- XXX: defaults

--- HTTP client request options.
--
-- Available HTTP client parameters may vary depending on
-- tarantool version. Refer to the HTTP client [documentation][1]
-- for precise list of available options.
--
-- [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/http/
--
-- @anchor http_client_request_opts
-- @string[opt]  ca_path
--     A path to an SSL certificate directory.
-- @string[opt]  ca_file
--     A path to an SSL certificate file.
-- @string[opt]  unix_socket
--     A path to a Unix domain socket.
-- @boolean[opt] verify_host
--     Whether to verify the certificate's name (CN) against host.
-- @boolean[opt] verify_peer
--     Whether to verify the peer's SSL certificate.
-- @string[opt]  ssl_key
--     Set path to the file with a private key for a TLS and SSL
--     client certificate.
-- @string[opt]  ssl_cert
--     Set path to the file with a SSL client certificate.
-- @string[opt]  proxy
--     Set a proxy to use.
-- @integer[opt] proxy_port
--     Set a port number the proxy listens on.
-- @string[opt]  proxy_user_pwd
--     Set a user name and a password to use in authentication.
-- @string[opt]  no_proxy
--     Disable proxy use for specific hosts.
-- @table[opt]   headers
--     HTTP headers. A table with strings keys and values, where
--     a key means an HTTP header name (a part before a colon) and
--     a value is the part after the colon.
-- @integer[opt] keepalive_idle
--     Amount of seconds the connection should be idle before
--     the client will send a keepalive probe.
--
--     Set it together with `keepalive_interval`, otherwise the
--     option will be ignored.
-- @integer[opt] keepalive_interval
--     Amount of seconds between keepalive probes.
--
--     Set it together with `keepalive_idle`, otherwise the
--     option will be ignored.
-- @integer[opt] low_speed_time
--     An average transfer speed in bytes per second that the
--     transfer should be below during `low_speed_limit` seconds
--     to consider it to be too slow and abort.
-- @integer[opt] low_speed_limit
--     See low_speed_time.
-- @number[opt]  timeout
--     Amount of seconds to wait for the response.
-- @integer[opt] max_header_name_length
--     Maximum length of a HTTP header name (a part before a
--     colon) in a response.
--
--     A longer name is truncated.
-- @boolean[opt] verbose
--     Whether to print debugging information to stderr.
-- @string[opt]  interface
--     Set network interface for outgoing connections.
--
--     May be an interface name, an IP addres or a host name.
--     See syntax in the libcurl [documentation][1].
--
--     [1]: https://curl.se/libcurl/c/CURLOPT_INTERFACE.html
-- @boolean[opt] follow_location
--    Whether to follow the 'Location' header that a server sends
--    as part of an 3xx response.
-- @string[opt]  accept_encoding
--    Set a list of accepted encodings, e.g. 'deflate, gzip'.
--    Enables automatic decompression of HTTP responses.
--
--    Supported encodings are 'deflate' and 'gzip' (may vary
--    depending on how tarantool is built). Use '' (an empty
--    string) to enable all supported encodings.
--
--    See more in the libcurl [documentation][2].
--
--    [2]: https://curl.se/libcurl/c/CURLOPT_ACCEPT_ENCODING.html
--
-- XXX: defaults where applicable

-- }}} HTTP options documentation

local http_error_mt
http_error_mt = {
    __concat = function(lhs, rhs)
        if getmetatable(lhs) == http_error_mt then
            return tostring(lhs) .. rhs
        elseif getmetatable(rhs) == http_error_mt then
            return lhs .. tostring(rhs)
        else
            error('conf.client.etcd.transport.http_error_mt.__concat(): ' ..
                'neither of args is an http error')
        end
    end,
    __tostring = function(self)
        return json.encode(self)
    end,
}

local function http_error_new(response)
    return setmetatable({
        response = response,
    }, http_error_mt)
end

local function http_error_ack(obj)
    if type(obj) == 'table' and getmetatable(obj) == http_error_mt then
        return obj
    end
    return nil
end

-- Those errors are documented in [1].
--
-- Here we hardcode error messages from [2]. They remain the same
-- since libcurl-7.17.0 (Sep 2007).
--
-- We can call curl_easy_strerror() using ffi, but I'm not sure
-- that the symbol is exposed on all tarantool versions / builds.
--
-- Hopefully tarantool will offer more stable way to differentiate
-- http client errors in future, see [3].
--
-- [1]: https://curl.se/libcurl/c/libcurl-errors.html
-- [2]: https://github.com/curl/curl/blob/curl-7_71_1/lib/strerror.c
-- [3]: https://github.com/tarantool/tarantool/issues/5916
local libcurl_transient_error_messages = {
    -- CURLE_COULDNT_RESOLVE_PROXY
    ["Couldn't resolve proxy name"] = true,
    -- CURLE_COULDNT_RESOLVE_HOST
    ["Couldn't resolve host name"] = true,
    -- CURLE_COULDNT_CONNECT
    ["Couldn't connect to server"] = true,
}

-- A request may be retried in the following cases (see [1]):
--
-- 1. UNAVAILABLE etcd error.
-- 2. libcurl errors, which certainly states that the request does
--    not reach the server (say, unable to connect).
-- 3. In case of a read request: any network / HTTP error (such
--    as timeout).
--
-- [1]: https://github.com/etcd-io/etcd/blob/v3.4.15/clientv3/retry.go#L46-L92
--
-- Returns true when the request may be retried after given error.
local function is_error_transient(err, request_is_read_only)
    if etcd_error.ack(err) then
        return err.code == etcd_error.UNAVAILABLE
    end
    if http_error_ack(err) then
        if request_is_read_only then
            return true
        end
        if libcurl_transient_error_messages[err.response.reason] then
            return true
        end
    end
    return false
end

-- Choose most descriptive error.
--
-- Let's consider an example cluster of three nodes:
--
-- | a  | b  | c  |
-- | up | up | up |
--
-- Let's assume that nodes 'a' and 'b' go down:
--
-- | a    | b    | c              |
-- | down | down | up (no quorum) |
--
-- Now we attempt to execute a request on all nodes and receive
-- the following errors:
--
-- a: network error CURLE_COULDNT_CONNECT (http error instance)
-- b: network error CURLE_COULDNT_CONNECT (http error instance)
-- c: etcd error UNAVAILABLE
--
-- Our strategy is round robin, but we can start from 'a', from
-- 'b' or from 'c' depending on previous failures. So, if we'll
-- choose a first or a last error, the error will be unpredictable
-- for given cluster state.
--
-- The solution is to report an etcd error if there are etcd errors
-- and http / network errors.
local function choose_more_detailed_error(err_1, err_2)
    if err_1 == nil then
        return err_2
    end
    if not etcd_error.ack(err_1) and etcd_error.ack(err_2) then
        return err_2
    end
    return err_1
end

-- Returns either:
--
-- - false, err
-- - true, response
local function request_current_node(self, location, encoded_request,
        attempt_num)
    local base_url = self.endpoints[self.endpoint_idx]
    local url = base_url .. location
    if attempt_num == 1 then
        log.verbose('etcd transport | request: %s %s', url, encoded_request)
    else
        log.verbose('etcd transport | request (attempt %d): %s %s', attempt_num,
            url, encoded_request)
    end
    local response = self.http_client:post(url, encoded_request,
        self.http_client_request_opts)
    log.verbose('etcd transport | response (%d): %s %s', response.status,
        location, response.body or '<no response body>')
    if response.status ~= 200 then
        local has_json_body = response.headers ~= nil and
            response.headers['content-type'] == 'application/json'
        if has_json_body then
            return false, etcd_error.new(json.decode(response.body))
        end
        return false, http_error_new(response)
    end
    return true, response.body
end

-- Retries the request in case of failure, when possible: see
-- is_error_transient() for details.
--
-- Raise http client internal errors (OOM, unknown libcurl error).
--
-- Raise HttpError or EtcdError.
local function request(self, location, request)
    local encoded_request = json.encode(request)
    local ok_acc
    local result_acc
    for attempt_num = 1, #self.endpoints do
        local ok, result = request_current_node(self, location, encoded_request,
            attempt_num)
        -- TODO: Differentiate read only and write requests.
        --
        -- Now all requests are assumed as ones that may write.
        if ok or not is_error_transient(result, false) then
            ok_acc = ok
            result_acc = result
            break
        end
        ok_acc = ok
        result_acc = choose_more_detailed_error(result_acc, result)
        self.endpoint_idx = self.endpoint_idx % #self.endpoints + 1
    end
    if not ok_acc then
        error(result_acc)
    end
    return json.decode(result_acc)
end

local mt = {
    __index = {
        request = request,
    }
}

-- Parameters:
--
-- opts.endpoints
-- opts.http_client.new
-- opts.http_client.request
local function new(opts)
    -- Merge given options with default ones.
    local opts = opts or {}
    local endpoints = opts.endpoints
    local http_client_opts = opts.http_client or {}
    local http_client_new_opts = utils.merge_deep(http_client_opts.new or {},
        http_client_new_opts_default)
    local http_client_request_opts = utils.merge_deep(http_client_opts.request
        or {}, http_client_request_opts_default)

    -- Verify the endpoints parameter.
    if endpoints == nil then
        error('endpoints is the mandatory parameter')
    end
    if type(endpoints) ~= 'table' then
        error(('endpoints parameter must be table, got %s'):format(
            type(endpoints)))
    end
    if endpoints[1] == nil then
        error('endpoints parameter must not be empty')
    end

    -- Create an HTTP client.
    local http_client = http_client_lib.new(http_client_new_opts)

    -- XXX: Handle user & password.

    return setmetatable({
        endpoint_idx = 1,
        endpoints = endpoints,
        http_client = http_client,
        http_client_request_opts = http_client_request_opts,
    }, mt)
end

return {
    new = new,
}
