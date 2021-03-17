--- HTTP transport for etcd client.
-- @module conf.driver.etcd.transport

-- TODO: Save cluster_id as first connect and validate it at
-- reconnects.

local log = require('log')
local json = require('json')
local http_client_lib = require('http.client')
local utils = require('conf.driver.etcd.utils')
local etcd_error = require('conf.driver.etcd.error')

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
            error('conf.driver.etcd.transport.http_error_mt.__concat(): ' ..
                'neither of args is an http error')
        end
    end,
    __tostring = function(self)
        return json.encode(self)
    end,
}

local function new_http_error(response)
    return setmetatable({
        response = response,
    }, http_error_mt)
end

-- Raise http client internal errors: OOM, unknown libcurl error.
--
-- Raise HttpError or EtcdError.
local function request(self, location, request)
    -- XXX: Round robin.
    local request = json.encode(request)
    log.verbose('etcd transport | request: %s %s', location, request)
    local base_url = self.endpoints[self.endpoint_idx]
    local url = base_url .. location
    local response = self.http_client:post(url, request,
        self.http_client_request_opts)
    log.verbose('etcd transport | response (%d): %s %s', response.status,
        location, response.body or '<no response body>')
    if response.status ~= 200 then
        local has_json_body = response.headers ~= nil and
            response.headers['content-type'] == 'application/json'
        if has_json_body then
            error(etcd_error.new(json.decode(response.body)))
        end
        -- TODO: There is no test for this branch.
        error(new_http_error(response))
    end
    return json.decode(response.body)
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
    local http_client_opts = opts.http_client or {}
    local http_client_new_opts = utils.merge_deep(http_client_opts.new or {},
        http_client_new_opts_default)
    local http_client_request_opts = utils.merge_deep(http_client_opts.request
        or {}, http_client_request_opts_default)

    -- XXX: Forbid empty endpoints.

    -- Create an HTTP client.
    local http_client = http_client_lib.new(http_client_new_opts)

    -- XXX: Handle user & password.

    return setmetatable({
        endpoint_idx = 1,
        endpoints = opts.endpoints,
        http_client = http_client,
        http_client_request_opts = http_client_request_opts,
    }, mt)
end

return {
    new = new,
}
