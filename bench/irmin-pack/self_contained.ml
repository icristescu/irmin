(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)
open Bench_common
open Irmin.Export_for_backends

module Store = struct
  open Tezos_context_hash.Encoding
  module Maker = Irmin_pack_layered.Maker_ext (Conf) (Node) (Commit)
  include Maker.Make (Metadata) (Contents) (Path) (Branch) (Hash)
end

let store_dir =
  "/Users/icristes/Documents/store/florencenet_lower102401_upper104449/context"

let min = "CoVqAEDV35ifEiSjxCopWZqPf8bXtDfxGB3tiJZqmZnbgdUV2X6Z"
let max = "CoWZenFAy19EHM98xtcidYMvAjA7iLPjsafuPEEKMKAMSVr6uM2U"

let create () =
  let conf = Irmin_pack.config ~readonly:false ~fresh:false store_dir in
  Store.Repo.v conf

let close repo =
  let* t, () = with_timer (fun () -> Store.Repo.close repo) in
  Logs.app (fun l -> l "close %f" t);
  Lwt.return_unit

let run () =
  let* repo = create () in
  let get_commit m =
    match Irmin.Type.(of_string Store.Hash.t) m with
    | Ok h -> (
      Fmt.epr "hash %a" (Irmin.Type.pp Store.Hash.t) h;
        Store.Commit.of_hash repo h >>= function
        | None -> Fmt.failwith "commit not found"
        | Some commit -> Lwt.return commit)
    | Error (`Msg m) -> Fmt.failwith "error min %s" m
  in
  let* min = get_commit min in
  let* max = get_commit max in

  Logs.app (fun l ->
      l "min %a max %a" Store.Commit.pp_hash min Store.Commit.pp_hash max);
  let* t, () = with_timer (fun () -> Store.self_contained ~min:[ min ] ~max:[ max ] repo) in
  let* () = close repo in
  Logs.app (fun l -> l "Executed in %f\nAfter self-contained command finished : " t);
  FSHelper.print_size_layers store_dir;
  Lwt.return_unit

let main () = Lwt_main.run (run ())

open Cmdliner

let setup_log =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let main_term = Term.(const main $ setup_log)

let () =
  let man =
    [
      `S "DESCRIPTION";
      `P
        "Benchmarks for the self contained operation of the layered store. \
         Requires the store at \
         /data/ioana/florencenet/florencenet_lower102401_upper104449.";
    ]
  in
  let info =
    Term.info ~man ~doc:"Benchmarks for self contained on layered stores."
      "self-contained "
  in
  Term.exit @@ Term.eval (main_term, info)
