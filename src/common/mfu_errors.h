/* Defines common error codes */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_ERRORS_H
#define MFU_ERRORS_H

/* Generic error codes */
#define ERR_INVAL_ARG EINVAL // 22

/* DAOS specific error codes*/
#define ERR_DAOS            40
#define ERR_DAOS_INVAL_PRFX 41

#endif /* MFU_ERRORS_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
