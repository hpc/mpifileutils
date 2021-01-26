LIST(APPEND man_pages
  dbcast.1
  dchmod.1
  dcmp.1
  dcp.1
  ddup.1
  dbz2.1
  dfind.1
  dreln.1
  drm.1
  dstripe.1
  dsync.1
  dtar.1
  dwalk.1
  )

INSTALL(FILES ${man_pages} DESTINATION ${X_DATADIR}/man/man1)
