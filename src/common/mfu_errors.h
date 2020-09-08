/* Defines common error codes */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_ERRORS_H
#define MFU_ERRORS_H

/* Convention:
 * 0-99    -> Generic error codes
 * 100-199 -> Utility-specific error codes
 * 200-255 -> Other error codes */

/* Generic error codes */
#define ERR_INVAL_ARG 10

/* DCP-specific error codes */
#define ERR_DCP      100
#define ERR_DCP_COPY 101

/* DAOS-specific error codes*/
#define ERR_DAOS            200
#define ERR_DAOS_INVAL_PRFX 201

#endif /* MFU_ERRORS_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
