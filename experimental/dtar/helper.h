/*
 * helper.h
 *
 *  Created on: Sep 10, 2013
 *      Author: fwang2
 */

#ifndef HELPER_H_
#define HELPER_H_

#include <pwd.h>
#include <grp.h>
#include <ctype.h>


char* userNameFromId(uid_t);
char* groupNameFromId(gid_t);

#endif /* HELPER_H_ */
