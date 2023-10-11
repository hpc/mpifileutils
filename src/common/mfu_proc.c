/* Defines a double linked list representing a file path. */

#include "mfu.h"

#include <unistd.h>

#include <errno.h>
#include <string.h>

#ifdef HAVE_LIBCAP
#include <sys/capability.h>
#endif

/* query properties and capabilities of current process */
void mfu_proc_set(mfu_proc_t* proc)
{
    proc->getuid  = getuid();
    proc->geteuid = geteuid();

    proc->cap_chown  = false;
    proc->cap_fowner = false;

#ifdef HAVE_LIBCAP
    /* query current process capability settings */
    cap_t caps = cap_get_proc();
    if (caps != NULL) {
        int rc;
        cap_flag_value_t flag;

        /* check for CAP_CHOWN */
        rc = cap_get_flag(caps, CAP_CHOWN, CAP_EFFECTIVE, &flag);
        if (rc == 0) {
            proc->cap_chown = (flag == CAP_SET);
        } else {
            MFU_LOG(MFU_LOG_ERR, "cap_get_flag(CAP_CHOWN) failed: errno=%d (%s)", errno, strerror(errno));
        }

        /* check for CAP_FOWNER */
        rc = cap_get_flag(caps, CAP_FOWNER, CAP_EFFECTIVE, &flag);
        if (rc == 0) {
            proc->cap_fowner = (flag == CAP_SET);
        } else {
            MFU_LOG(MFU_LOG_ERR, "cap_get_flag(CAP_FOWNER) failed: errno=%d (%s)", errno, strerror(errno));
        }

        /* free capabilities structure */
        rc = cap_free(caps);
        if (rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "cap_free() failed: errno=%d (%s)", errno, strerror(errno));
        }
    } else {
        MFU_LOG(MFU_LOG_ERR, "cap_get_proc() failed: errno=%d (%s)", errno, strerror(errno));
    }
#endif
}
