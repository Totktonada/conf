# conf

## This branch (story-of-birth)

This branch contains git history from the early development stage of the
module. Unlikely it'll be interested for someone, but kept here just in case.

## Status

Early beta.

It is okay to develop an application based on this module if it is planned for
production not before Q3'21.

Not ready for production usage.

## Status (details)

There are several reasons why the module is not ready for production usage ATM:

* At this stage we leave a room for Lua API changes.
* Moreover, we still in thoughts about a best data representation in etcd. If
  we'll change it, old data may become unaccessible.
* Tested only with etcd 3.4.14. There are known problems with older etcd
  versions.
* No TLS support.
* No authentication support.
* Operations are not transactional.
* It is just published, not so well tested.

Aside of this, there are several important features that are not implemented
ATM. They may be important in some usage scenarious:

* No leases.
* No way to acquire / compare a revision or version of data.
* No way to coalesce several get/set/del operations into one transaction.

The rough plan is to resolve most of such hot questions in Q2'21.

## Installation

```sh
tarantoolctl rocks install conf
```

## Usage

Consider the [API documentation][apidoc].

[apidoc]: https://tarantool.github.io/conf/

## License

BSD 2 clause.

See the LICENSE file.
