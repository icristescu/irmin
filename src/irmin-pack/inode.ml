(*
 * Copyright (c) 2013-2019 Thomas Gazagnaire <thomas@gazagnaire.org>
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

open Lwt.Infix
include Inode_intf

let src =
  Logs.Src.create "irmin.pack.i" ~doc:"inodes for the irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

module Make
    (Conf : Config.S)
    (H : Irmin.Hash.S)
    (Pack : Pack.MAKER with type key = H.t)
    (Node : Irmin.Private.Node.S with type hash = H.t) =
struct
  type index = Pack.index

  module Common = Inode_common.Make_intermediary (Conf) (H) (Node)
  module Node = Common.Node
  module T = Common.T

  module Inode = struct
    module StepMap = Common.Inode.StepMap
    module Bin = Common.Inode.Bin
    module Compress = Common.Inode.Compress
    module Val = Common.Inode.Val
    include Pack.Make (Common.Inode.PackElt)
  end

  module Val = Common.Val
  module Key = H

  type 'a t = 'a Inode.t

  type key = Key.t

  type value = Val.t

  let mem t k = Inode.mem t k

  let unsafe_find t k =
    match Inode.unsafe_find t k with
    | None -> None
    | Some v ->
        let v = Inode.Val.of_bin v in
        Some v

  let find t k =
    Inode.find t k >|= function
    | None -> None
    | Some v ->
        let v = Inode.Val.of_bin v in
        let find = unsafe_find t in
        Some { Val.find; v }

  let save t v =
    let add k v = Inode.unsafe_append t k v in
    Inode.Val.save ~add ~mem:(Inode.unsafe_mem t) v

  let hash v = Lazy.force v.Val.v.Inode.Val.hash

  let add t v =
    save t v.Val.v;
    Lwt.return (hash v)

  let check_hash expected got =
    if Irmin.Type.equal H.t expected got then ()
    else
      Fmt.invalid_arg "corrupted value: got %a, expecting %a" T.pp_hash expected
        T.pp_hash got

  let unsafe_add t k v =
    check_hash k (hash v);
    save t v.Val.v;
    Lwt.return_unit

  let batch = Inode.batch

  let v = Inode.v

  type integrity_error = Inode.integrity_error

  let integrity_check = Inode.integrity_check

  let close = Inode.close

  let sync = Inode.sync

  let clear = Inode.clear

  let clear_caches = Inode.clear_caches
end
