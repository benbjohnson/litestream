Litestream
![GitHub release (latest by date)](https://img.shields.io/github/v/release/benbjohnson/litestream)
![Status](https://img.shields.io/badge/status-beta-blue)
![GitHub](https://img.shields.io/github/license/benbjohnson/litestream)
[![Docker Pulls](https://img.shields.io/docker/pulls/litestream/litestream.svg?maxAge=604800)](https://hub.docker.com/r/litestream/litestream/)
==========

Litestream is a standalone disaster recovery tool for SQLite. It runs as a
background process and safely replicates changes incrementally to another file
or S3. Litestream only communicates with SQLite through the SQLite API so it
will not corrupt your database.

If you need support or have ideas for improving Litestream, please visit
[GitHub Issues](https://github.com/benbjohnson/litestream/issues).
Please visit the [Litestream web site](https://litestream.io) for installation
instructions and documentation.

If you find this project interesting, please consider starring the project on
GitHub.

Source database changes
-----------------------

When Litestream begins replicating a database, it creates an internal
`_litestream_lock` table in the source SQLite database. This is expected and
safe to leave in place. Litestream uses the table to acquire SQLite's write
lock while coordinating synchronization around WAL checkpoints. The writes
occur in transactions that are rolled back, so Litestream does not leave rows
in the table.

The table is part of the source database, not a separate table in the replica
destination. Because the source schema is backed up, restored databases also
contain it. Its creation changes the source database schema and can change its
page count or bytes, so a database under Litestream should not be expected to
remain byte-identical to its pre-Litestream state.

Do not drop `_litestream_lock` while Litestream is running. Synchronization
around a checkpoint can fail until Litestream reinitializes the database.
Restarting Litestream recreates the missing table automatically.

Contributing
------------

We welcome bug reports, fixes, and patches! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to contribute.

Security
--------

Please do not open a public issue for security vulnerabilities. Report them
privately through GitHub's [private vulnerability reporting](https://github.com/benbjohnson/litestream/security/advisories/new),
which keeps the report visible only to you and the maintainers until a fix is
released. See our [Security Policy](SECURITY.md) for what to include and what to
expect.

Acknowledgements
----------------

I want to give special thanks to individuals who invest much of their time and
energy into the project to help make it better:

- Thanks to [Cory LaNou](https://twitter.com/corylanou) for giving early feedback and testing when Litestream was still pre-release.
- Thanks to [Michael Lynch](https://github.com/mtlynch) for digging into issues and contributing to the documentation.
- Thanks to [Kurt Mackey](https://twitter.com/mrkurt) for feedback and testing.
- Thanks to [Sam Weston](https://twitter.com/cablespaghetti) for figuring out how to run Litestream on Kubernetes and writing up the docs for it.
- Thanks to [Rafael](https://github.com/netstx) & [Jungle Boogie](https://github.com/jungle-boogie) for helping to get OpenBSD release builds working.
- Thanks to [Simon Gottschlag](https://github.com/simongottschlag), [Marin](https://github.com/supermarin),[Victor Björklund](https://github.com/victorbjorklund), [Jonathan Beri](https://twitter.com/beriberikix) [Yuri](https://github.com/yurivish), [Nathan Probst](https://github.com/nprbst), [Yann Coleu](https://github.com/yanc0), and [Nicholas Grilly](https://twitter.com/ngrilly) for frequent feedback, testing, & support.

Huge thanks to fly.io for their support and for contributing credits for testing and development!
