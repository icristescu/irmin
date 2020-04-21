(*
 * Copyright (c) 2013-2020 Thomas Gazagnaire <thomas@gazagnaire.org>
 *                         Ioana Cristescu <ioana@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESIrmin. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "irmin.layered" ~doc:"Irmin layered store"

module Log = (val Logs.src_log src : Logs.LOG)

open Lwt.Infix

module type LAYERED_S = sig
  include Pack.S

  module U : Pack.S

  type 'a upper = 'a U.t

  val v :
    'a upper ->
    ?fresh:bool ->
    ?readonly:bool ->
    ?lru_size:int ->
    index:index ->
    string ->
    'a t Lwt.t

  val batch : unit -> 'a Lwt.t

  val project : 'a t -> [ `Read | `Write ] upper -> [ `Read | `Write ] t

  val layer_id : [ `Read ] t -> key -> int Lwt.t

  val freeze : unit -> unit
end

module Content_addressable
    (Index : Pack_index.S)
    (S : Pack.S with type index = Index.t) :
  LAYERED_S
    with type key = S.key
     and type value = S.value
     and type index = S.index
     and module U = S = struct
  type index = S.index

  type key = S.key

  type value = S.value

  type 'a upper = 'a S.t

  type 'a t = { upper : 'a upper; lower : [ `Read ] S.t }

  module U = S

  let v upper ?fresh ?readonly ?lru_size ~index root =
    S.v ?fresh ?readonly ?lru_size ~index root >|= fun lower -> { upper; lower }

  let add t = S.add t.upper

  let unsafe_add t = S.unsafe_add t.upper

  let unsafe_append t k v = S.unsafe_append t.upper k v

  let find t k =
    S.find t.upper k >>= function
    | None -> S.find t.lower k
    | Some v -> Lwt.return_some v

  let unsafe_find t k =
    match S.unsafe_find t.upper k with
    | None -> S.unsafe_find t.lower k
    | Some v -> Some v

  let mem t k =
    S.mem t.upper k >>= function
    | true -> Lwt.return_true
    | false -> S.mem t.lower k

  let unsafe_mem t k = S.unsafe_mem t.upper k || S.unsafe_mem t.lower k

  let project t upper = { t with upper }

  let batch () = failwith "don't call this function"

  let sync t =
    S.sync t.upper;
    S.sync t.lower

  let close t = S.close t.upper >>= fun () -> S.close t.lower

  type integrity_error = S.integrity_error

  let integrity_check ~offset ~length k t =
    let upper = t.upper in
    let lower = t.lower in
    match
      ( S.integrity_check ~offset ~length k upper,
        S.integrity_check ~offset ~length k lower )
    with
    | Ok (), Ok () -> Ok ()
    | Error `Wrong_hash, _ | _, Error `Wrong_hash -> Error `Wrong_hash
    | Error `Absent_value, _ | _, Error `Absent_value -> Error `Absent_value

  let freeze () = ()

  let layer_id t k =
    S.mem t.upper k >>= function
    | true -> Lwt.return 1
    | false -> (
        S.mem t.lower k >|= function true -> 2 | false -> raise Not_found )
end

module type LAYERED_MAKER = sig
  type key

  type index

  module Make (V : Pack.ELT with type hash := key) :
    LAYERED_S
      with type key = key
       and type value = V.t
       and type index = index
       and type U.index = index
end

module Pack_Maker
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.MAKER with type key = H.t and type index = Index.t) :
  LAYERED_MAKER with type key = S.key and type index = S.index = struct
  type index = S.index

  type key = S.key

  module Make (V : Pack.ELT with type hash := key) = struct
    module Upper = S.Make (V)
    include Content_addressable (Index) (Upper)
  end
end

module type AW = sig
  include Irmin.ATOMIC_WRITE_STORE

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end

module Atomic_write (A : AW) : sig
  include AW with type key = A.key and type value = A.value

  val v : A.t -> ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end = struct
  type key = A.key

  type value = A.value

  type t = { upper : A.t; lower : A.t }

  module U = A

  let mem t k =
    U.mem t.upper k >>= function
    | true -> Lwt.return_true
    | false -> A.mem t.lower k

  let find t k =
    U.find t.upper k >>= function
    | None -> A.find t.lower k
    | Some v -> Lwt.return_some v

  let set t = U.set t.upper

  (* we have to copy back into upper the branch against we want to do
     test and set *)
  let test_and_set t k ~test ~set =
    U.mem t.upper k >>= function
    | true -> U.test_and_set t.upper k ~test ~set
    | false -> (
        A.find t.lower k >>= function
        | None -> U.test_and_set t.upper k ~test:None ~set
        | Some v ->
            U.set t.upper k v >>= fun () -> U.test_and_set t.upper k ~test ~set
        )

  let remove t k = U.remove t.upper k >>= fun () -> A.remove t.lower k

  let list t =
    U.list t.upper >>= fun upper ->
    A.list t.lower >|= fun lower ->
    List.fold_left
      (fun acc b -> if List.mem b acc then acc else b :: acc)
      lower upper

  type watch = U.watch

  let watch t = U.watch t.upper

  let watch_key t = U.watch_key t.upper

  let unwatch t = U.unwatch t.upper

  let close t = U.close t.upper >>= fun () -> A.close t.lower

  let v upper ?fresh ?readonly file =
    A.v ?fresh ?readonly file >|= fun lower -> { upper; lower }
end
