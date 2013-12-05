#ifndef _COMMON_H
#define _COMMON_H

struct
{
	int maxdepth;
	char * root;
} options;

#undef DEBUG

#ifdef DEBUG
#define dbprintf(...) do { fprintf(stderr, __VA_ARGS__); } while(0)
#else
#define dbprintf(...) do {} while (0)
#endif

#endif
