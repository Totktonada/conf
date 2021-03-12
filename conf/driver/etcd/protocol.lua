local grpc_json = require('conf.driver.etcd.grpc_json')

local function new()
    local protocol = grpc_json.new_protocol()

    -- https://github.com/etcd-io/etcd/blob/v3.4.15/mvcc/mvccpb/kv.proto
    -- https://github.com/etcd-io/etcd/blob/v3.4.15/etcdserver/etcdserverpb/rpc.proto

    -- {{{ General purpose messages

    protocol:add_message('ResponseHeader', {
        [1] = {'uint64', 'cluster_id'},
        [2] = {'uint64', 'member_id'},
        [3] = {'int64', 'revision'},
        [4] = {'uint64', 'raft_term'},
    })

    protocol:add_message('KeyValue', {
        [1] = {'bytes', 'key'},
        [2] = {'int64', 'create_revision'},
        [3] = {'int64', 'mod_revision'},
        [4] = {'int64', 'version'},
        [5] = {'bytes', 'value'},
        [6] = {'int64', 'lease'},
    })

    -- }}} General purpose messages

    -- {{{ Range request / response

    protocol:add_enum('SortOrder', {
        [0] = 'NONE',
        [1] = 'ASCEND',
        [2] = 'DESCEND',
    })

    protocol:add_enum('SortTarget', {
        [0] = 'KEY',
        [1] = 'VERSION',
        [2] = 'CREATE',
        [3] = 'MOD',
        [4] = 'VALUE',
    })

    protocol:add_message('RangeRequest', {
        [1] = {'bytes', 'key'},
        [2] = {'bytes', 'range_end'},
        [3] = {'int64', 'limit'},
        [4] = {'int64', 'revision'},
        [5] = {'SortOrder', 'sort_order'},
        [6] = {'SortTarget', 'sort_target'},
        [7] = {'bool', 'serializable'},
        [8] = {'bool', 'keys_only'},
        [9] = {'bool', 'count_only'},
        [10] = {'int64', 'min_mod_revision'},
        [11] = {'int64', 'max_mod_revision'},
        [12] = {'int64', 'min_create_revision'},
        [13] = {'int64', 'max_create_revision'},
    })

    protocol:add_message('RangeResponse', {
        [1] = {'ResponseHeader', 'header'},
        [2] = {'repeated', 'KeyValue', 'kvs'},
        [3] = {'bool', 'more'},
        [4] = {'int64', 'count'},
    })

    -- }}}

    -- {{{ Put request / response

    protocol:add_message('PutRequest', {
      [1] = {'bytes', 'key'},
      [2] = {'bytes', 'value'},
      [3] = {'int64', 'lease'},
      [4] = {'bool', 'prev_kv'},
      [5] = {'bool', 'ignore_value'},
      [6] = {'bool', 'ignore_lease'},
    })

    protocol:add_message('PutResponse', {
      [1] = {'ResponseHeader', 'header'},
      [2] = {'KeyValue', 'prev_kv'},
    })

    -- }}} Put request / response

    return protocol
end

return {
    new = new,
}
