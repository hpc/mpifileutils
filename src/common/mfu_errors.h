/* Defines common error codes */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_ERRORS_H
#define MFU_ERRORS_H

/* Generic error codes */
#define MFU_ERR           1000
#define MFU_ERR_INVAL_ARG 1001

/* DCP-specific error codes */
#define MFU_ERR_DCP      1100
#define MFU_ERR_DCP_COPY 1101

/* DAOS-specific error codes*/
#define MFU_ERR_DAOS            4000
#define MFU_ERR_DAOS_INVAL_ARG  4001

/* Error macros */
#define MFU_ERRF "%s(%d)"
#define MFU_ERRP(rc) "MFU_ERR", rc

#endif /* MFU_ERRORS_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
