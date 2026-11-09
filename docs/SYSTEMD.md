# Optional systemd hardening

The packaged `litestream.service` keeps its existing defaults. Hardening is
opt-in because database locations, replica destinations, permissions, credential
helpers, and commands launched with `-exec` vary between installations.

The example `etc/litestream-hardening.conf.example` requires systemd 232 or
newer and a system service with mount namespace support. Confirm support on the
target host with `systemd --version` and `systemd-analyze verify`; unknown
directives are not evidence that confinement is active. See
[systemd.exec](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)
for directive semantics and platform requirements.

## Adapt the example

Read the example before installing it. Its `ReadWritePaths` values are example
directories that must be replaced with existing paths for your deployment.
`ProtectSystem=strict` makes the filesystem read-only except for explicitly
writable paths and systemd's API filesystem exceptions.

Include every directory Litestream or its child processes must write:

- Each database's parent directory, including its `-wal`, `-shm`, and default
  `.<database>-litestream` metadata directory.
- Any custom `meta-path` directory and each local file replica directory.
- Directories for configured logs, the control socket, temporary files, restore
  output, and files written by hooks or credential helpers, as applicable.

Allowing only the database file is insufficient: SQLite and Litestream create
and rename adjacent files. Create the directories before starting the service.
Filesystem allowlists do not grant Unix ownership or permission access.

The example keeps the service's existing user, groups, capabilities, umask,
network access, and `/proc` visibility. To run as an unprivileged account, set
`User=` and `Group=` in the drop-in and grant that account read/write access to
the database directories and access to its replica credentials. Verify that the
application can still write the database and its WAL and SHM files.

All confinement is inherited by commands launched with `-exec`, credential
helpers, and their children. `NoNewPrivileges=true` prevents gaining privileges
through setuid executables or file capabilities. The kernel protections and
`LockPersonality`/`RestrictRealtime` settings can also affect child programs;
test them with your actual application and helpers.

## Install and validate

Release archives include the example under `etc/` and this guide under `docs/`.
Debian and RPM packages install them under
`/usr/share/doc/litestream/examples/` and `/usr/share/doc/litestream/`.
Package installation does not enable the example.

After adapting the example, install it on the target Linux host:

```bash
sudo install -d -m 0755 /etc/systemd/system/litestream.service.d
sudo install -m 0644 litestream-hardening.conf.example /etc/systemd/system/litestream.service.d/hardening.conf
sudo systemctl daemon-reload
systemd-analyze verify litestream.service
systemctl cat litestream.service
sudo systemctl restart litestream.service
journalctl -u litestream.service
```

Verify that transactions replicate to the configured destination, restore into
a separate allowed directory, and check the restored database. Exercise every
configured hook, credential helper, and control socket client. Check for
permission, read-only filesystem, and namespace errors in the journal. A
`systemd-analyze security litestream.service` score alone does not establish
that replication or recovery works.

To disable the example, rename `hardening.conf` to `hardening.conf.disabled`,
reload the systemd manager, and restart the service. Preserve any other drop-ins.

## Additional restrictions

Add stronger settings only after validating them on your deployment:

- `PrivateTmp=true` gives child processes a private `/tmp` and `/var/tmp`, which
  can hide a control socket or application files from external clients.
- `ProtectHome=true` can hide databases or credentials stored in home directories.
- `ProcSubset=pid` hides system information used by process metrics.
- `RestrictSUIDSGID=true` can prevent creation of setgid metadata directories.
- `SystemCallFilter` and `RestrictAddressFamilies` can break child programs or
  credential and network helpers; the `@system-service` syscall group requires
  systemd 239 or newer.
- `MemoryDenyWriteExecute=true` can break hooks that use a JIT runtime.
- `CapabilityBoundingSet` and a restrictive `UMask` can change database access,
  restored-file permissions, or application behavior.

These settings are deliberately absent from the example. Test recovery and
application behavior after each additional restriction.
