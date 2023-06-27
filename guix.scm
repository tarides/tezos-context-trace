(use-modules (guix packages)
	     (guix gexp)
	     (guix git)
	     (guix git-download)
	     (tarides packages tezos)
	     (tarides packages irmin))

(package-with-irmin-3.8
 (package-with-tezos-16
  ocaml-tezos-context-trace
  ;; use a verion of Tezos that uses mtime 2.0.0
  #:origin (origin
	    (method git-fetch)
	    (uri (git-reference
		  (url "https://github.com/adatario/tezos")
		  (commit "a1d32e306ab4d8ae56d2836dbf03168829b4e128")))
	    (sha256
	     (base32
	      "1mvy721bscavbwyf7m4ydf8lxpzndqj04hk2gs1idk5wr8qv6mv1"))
	    (patches
	     (list (local-file
		    "0001-Lib_context-Add-irmin-stats.patch"))))))
