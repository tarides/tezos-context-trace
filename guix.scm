(use-modules
 (guix packages)
 (guix build-system dune)
 (guix git)
 (guix git-download)
 ((guix licenses) #:prefix license:)
 (gnu packages admin)
 (gnu packages ocaml)
 (tarides packages ocaml)
 (tarides packages irmin)
 (tarides packages tezos))

(define tezos-origin
  (let ((commit "763259c5131a5cc8054151596f0f59ffb505f0fc")
	;; only used for package naming
	(version "15.1"))
    (origin
     (method git-fetch)
     (uri (git-reference
	   (url "https://gitlab.com/tezos/tezos.git")
	   (commit commit)))
     (file-name (git-file-name "tezos" version))
     (sha256
      (base32
       "0lvf226hahlflfvyx9jh65axvain7lhzi6m62ih3pbwi72d600wr")))))

;; only used for package naming
(define tezos-version "15.1")

(define irmin-tezos-benchmarking
  (package
   (name "irmin-tezos-benchmarking")
   (version "0.0.0")
   (home-page #f)
   (source (git-checkout (url (dirname (current-filename)))))
   (build-system dune-build-system)
   (propagated-inputs
    (list

     ;; tezos-contest (aka lib_context)
     ocaml-tezos-context

     ;; Extra dependencies required by the replay patches
     ocaml-ppx-deriving
     ocaml-ppx-deriving-yojson
     ocaml-printbox
     ocaml-bentov))
   (native-inputs
    (list
     ;; Ansible for deploying benchmark servers
     ansible))
   (synopsis #f)
   (description #f)
   (license license:isc)))

(package-with-explicit-tezos-origin
 (package-with-irmin-3.4
  irmin-tezos-benchmarking)
 #:origin tezos-origin
 #:version tezos-version)
