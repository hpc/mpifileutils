/* Compile-only stand-in for the HPSSFS-FUSE header, which is not public.
 * The ioctl number is arbitrary: never run a binary built with this
 * against real HPSS. */

#ifndef _LINUX_HPSSFS_H
#define _LINUX_HPSSFS_H

#include <linux/ioctl.h>
#include <stdint.h>

#define HPSSFS_SET_FSIZE_HINT _IOW(0x48, 1, uint64_t)

#endif /* _LINUX_HPSSFS_H */
