# Znapper - Znap your Snaps

Znapper is a tool to help automate zfs snapshot management on larger pools. This is similar to
zfs-auto-snap, but goes a bit further to help manage snapshots over replicated pools for backups
within a single host.

## Auto snapshot management

To automatically snapshot *all* pools on your system which have *mounted* filesystems:

```
znapper snapshot [filesystem name]...
znapper snapshot
znapper snapshot tank
znapper snapshot tank tonk tunk
znapper snapshot tank/pab tonk/pob tunk/pub
```

> NOTE: Only the mounted filesystems are snapshot. If it has no mountpoint, for example:

```
NAME                                             USED  AVAIL     REFER  MOUNTPOINT
nvme                                             388G   228G       96K  none
```

This filesystem will NOT be snapshot, but it's descendants that are mounted will be!

To clean-up old automatic snapshots

```
znapper snapshot_cleanup <poolname> <hours worth of snapshots to keep>
znapper snapshot_cleanup tank 48
```

## Replication management

This is really what znapper was designed to do. Let's say you have two pools, a smaller nvme pool
and a larger disk backed pool named tank.

On the smaller nvme pool, because it's smaller we can not keep as many snapshots due to space
limitations. We want to replicate nvme to tank for archival / redundancy, but also to allow extended
snapshots to be stored in tank.

To initialise the replication:

```
znapper init_repl <from filesystem> <to filesystem>
znapper init_repl nvme tank/nvme
```

To then do an incremental replication

```
znapper repl <from filesystem> <to filesystem>
znapper repl nvme tank/nvme
```

> NOTE: local repl assumes the "to" filesystem is an exact match of name and structure. An example is:

```
# WOULD FAIL!
znapper init_repl nvme/descendant tank/repl/nvme

# Would work!
znapper init_repl nvme/descendant tank/repl/nvme/descendant
```

# How does it work? 

The reason auto snapshot only snapshots mounted filesystems is so that any replication target (ie
tank/nvme) is NOT snapshot hourly in the background.

This is also why snapshot cleanup requires a pool name, because when snapshots are sent from nvme
to tank, we can then cleanup tank/nvme's auto snapshots based on tank's policy which can be
longer than the nvme policy.

Replication uses different snapshots as points in times for replication, so even removing all the
auto snapshots on either side will NOT break the replication process.

Any replicated filesystem is *not* mounted and marked as read-only in the process. To restore from
one of these snapshots, you can either zfs send back to the original pool, or temporarily mount
the fs to manually recover.

# Example systemd service files to automate this process.

```
# zfs-auto-snapshot-hourly.service
[Unit]
Description=ZFS hourly snapshot service

[Service]
Type=oneshot
ExecStart=znapper snapshot
ExecStart=znapper snapshot_cleanup tank 72
ExecStart=znapper snapshot_cleanup nvme 24
```

```
zfs-auto-snapshot-hourly.timer
[Unit]
Description=ZFS hourly snapshot timer

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

```
zfs-auto-replicate-daily.service
[Unit]
Description=ZFS daily replicate service

[Service]
Type=oneshot
ExecStart=znapper repl nvme tank/nvme
```

> NOTE: Due to the "only mounted" filesystems are snapshot behaviour, when you replicate nvme to
> tank/nvme here, the repl target of tank/nvme will NOT be snapshot, but the snapshots of nvme
> are preserved!

```
zfs-auto-replicate-daily.timer
[Unit]
Description=ZFS daily replicate timer

[Timer]
OnCalendar=*-*-* 18:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

