#ifndef MFU_PROC_H
#define MFU_PROC_H

#include <unistd.h>
#include <sys/types.h>

/* records properties about the current process */
typedef struct mfu_proc_struct {
  uid_t getuid;
  uid_t geteuid;
  bool cap_chown;
  bool cap_fowner;
} mfu_proc_t;

/* query and cache values for current process */
void mfu_proc_set(mfu_proc_t* proc);

#endif /* MFU_PROC_H */
