	a = archive_write_new();
	switch (compress) {
	case 'j': case 'y':
		archive_write_add_filter_bzip2(a);
		break;
	case 'Z':
		archive_write_add_filter_compress(a);
		break;
	case 'z':
		archive_write_add_filter_gzip(a);
		break;
	default:
		printf("I am here\n");
		archive_write_add_filter_none(a);
		break;
	}
	archive_write_set_format_ustar(a);
        archive_write_set_bytes_per_block(a,0);


	if (strcmp(filename, "-") == 0)
		filename = NULL;
    
	/* method 1  */
	flags=O_WRONLY | O_CREAT | O_TRUNC | O_BINARY | O_CLOEXEC;
        fd2=open(filename,flags,0666);
        archive_write_open_fd(a, fd2);

        /* method 2 */
        /* archive_write_open_filename(a, filename);*/

	disk = archive_read_disk_new();
	archive_read_disk_set_standard_lookup(disk);
	while (*argv != NULL) {
		struct archive *disk = archive_read_disk_new();
		int r;

		r = archive_read_disk_open(disk, *argv);
		if (r != ARCHIVE_OK) {
			errmsg(archive_error_string(disk));
			errmsg("\n");
			exit(1);
		}

		for (;;) {
			int needcr = 0;

			entry = archive_entry_new();
			r = archive_read_next_header2(disk, entry);
			if (r == ARCHIVE_EOF)
				break;
			if (r != ARCHIVE_OK) {
				errmsg(archive_error_string(disk));
				errmsg("\n");
				exit(1);
			}
			archive_read_disk_descend(disk);
			if (verbose) {
				msg("a ");
				msg(archive_entry_pathname(entry));
				needcr = 1;
			}
			r = archive_write_header(a, entry);
                        write(fd2,"I am after header",18);

			if (r < ARCHIVE_OK) {
				errmsg(": ");
				errmsg(archive_error_string(a));
				needcr = 1;
			}
			if (r == ARCHIVE_FATAL)
				exit(1);
			if (r > ARCHIVE_FAILED) {

				/* For now, we use a simpler loop to copy data
				 * into the target archive. */
				fd = open(archive_entry_sourcepath(entry), O_RDONLY);
				len = read(fd, buff, sizeof(buff));
				while (len > 0) {
					archive_write_data(a, buff, len);
					len = read(fd, buff, sizeof(buff));
				}
				close(fd);
			}
			archive_entry_free(entry);
			if (needcr)
				msg("\n");
		}
      
                write(fd2,"I am after data",16);

		archive_read_close(disk);
		archive_read_free(disk);
		argv++;
	}

	archive_write_close(a);
	archive_write_free(a);

}

static void
extract(const char *filename, int do_extract, int flags)
{
	struct archive *a;
	struct archive *ext;
	struct archive_entry *entry;
	int r;

	a = archive_read_new();
	ext = archive_write_disk_new();
	archive_write_disk_set_options(ext, flags);
	archive_read_support_filter_bzip2(a);
	archive_read_support_filter_gzip(a);
	archive_read_support_filter_compress(a);
	archive_read_support_format_tar(a);
	archive_read_support_format_cpio(a);
	archive_write_disk_set_standard_lookup(ext);

	if (filename != NULL && strcmp(filename, "-") == 0)
		filename = NULL;
	if ((r = archive_read_open_filename(a, filename, 10240))) {
		errmsg(archive_error_string(a));
		errmsg("\n");
		exit(r);
	}
	for (;;) {
		r = archive_read_next_header(a, &entry);
		if (r == ARCHIVE_EOF)
			break;
		if (r != ARCHIVE_OK) {
			errmsg(archive_error_string(a));
			errmsg("\n");
			exit(1);
		}
		if (verbose && do_extract)
			msg("x ");
		if (verbose || !do_extract)
			msg(archive_entry_pathname(entry));
		if (do_extract) {
			r = archive_write_header(ext, entry);
			if (r != ARCHIVE_OK)
				errmsg(archive_error_string(a));
			else
				copy_data(a, ext);
		}
		if (verbose || !do_extract)
			msg("\n");
	}
	archive_read_close(a);
	archive_read_free(a);
	exit(0);
}

static int
copy_data(struct archive *ar, struct archive *aw)
{
	int r;
	const void *buff;
	size_t size;
	int64_t offset;

	for (;;) {
		r = archive_read_data_block(ar, &buff, &size, &offset);
		if (r == ARCHIVE_EOF) {
			errmsg(archive_error_string(ar));
			return (ARCHIVE_OK);
		}
		if (r != ARCHIVE_OK)
			return (r);
		r = archive_write_data_block(aw, buff, size, offset);
		if (r != ARCHIVE_OK) {
			errmsg(archive_error_string(ar));
			return (r);
		}
	}
}

static void
msg(const char *m)
{
	write(1, m, strlen(m));
}

static void
errmsg(const char *m)
{
	if (m == NULL) {
		m = "Error: No error description provided.\n";
	}
	write(2, m, strlen(m));
}

static void
usage(void)
{
/* Many program options depend on compile options. */
	const char *m = "Usage: minitar [-"
	    "c"
	    "j"
	    "tvx"
	    "y"
	    "Z"
	    "z"
	    "] [-f file] [file]\n";

	errmsg(m);
	exit(1);
}
