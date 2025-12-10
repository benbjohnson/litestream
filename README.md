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

If you need support or have ideas for improving Litestream, please join the
[Litestream Slack][slack] or visit the [GitHub Discussions](https://github.com/benbjohnson/litestream/discussions).
Please visit the [Litestream web site](https://litestream.io) for installation
instructions and documentation.

If you find this project interesting, please consider starring the project on
GitHub.

[slack]: https://join.slack.com/t/litestream/shared_invite/zt-n0j4s3ci-lx1JziR3bV6L2NMF723H3Q

Contributing
------------

We welcome bug reports, fixes, and patches! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to contribute.

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
