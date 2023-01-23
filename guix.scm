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

(define irmin-tezos-benchmarking
  (package-with-tezos-16
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
     (synopsis #f)
     (description #f)
     (license license:isc))))

irmin-tezos-benchmarking