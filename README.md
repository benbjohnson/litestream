Litestream
![GitHub release (latest by date)](https://img.shields.io/github/v/release/benbjohnson/litestream)
![Status](https://img.shields.io/badge/status-beta-blue)
![GitHub](https://img.shields.io/github/license/benbjohnson/litestream)
![test](https://github.com/benbjohnson/litestream/workflows/test/badge.svg)
==========

Litestream is a standalone streaming replication tool for SQLite. It runs as a
background process and safely replicates changes incrementally to another file
or S3. Litestream only communicates with SQLite through the SQLite API so it
will not corrupt your database.

If you need support or have ideas for improving Litestream, please visit the
[GitHub Discussions](https://github.com/benbjohnson/litestream/discussions) to
chat. 

If you find this project interesting, please consider starring the project on
GitHub.

Please visit the [Litestream web site](https://litestream.io) for installation
instructions and documentation.


## Acknowledgements

While the Litestream project does not accept external code patches, many
of the most valuable contributions are in the forms of testing, feedback, and
documentation. These help harden software and streamline usage for other users.

I want to give special thanks to individuals who invest much of their time and 
energy into the project to help make it better. Shout out to [Michael
Lynch](https://github.com/mtlynch) for digging into issues and contributing to
the documentation.


## Open-source, not open-contribution

[Similar to SQLite](https://www.sqlite.org/copyright.html), Litestream is open
source but closed to code contributions. This keeps the code base free of
proprietary or licensed code but it also helps me continue to maintain and build
Litestream.

As the author of [BoltDB](https://github.com/boltdb/bolt), I found that
accepting and maintaining third party patches contributed to my burn out and
I eventually archived the project. Writing databases & low-level replication
tools involves nuance and simple one line changes can have profound and
unexpected changes in correctness and performance. Small contributions
typically required hours of my time to properly test and validate them.

I am grateful for community involvement, bug reports, & feature requests. I do
not wish to come off as anything but welcoming, however, I've
made the decision to keep this project closed to contributions for my own
mental health and long term viability of the project.

The [documentation repository][docs] is MIT licensed and pull requests are welcome there.

[releases]: https://github.com/benbjohnson/litestream/releases
[docs]: https://github.com/benbjohnson/litestream.io

