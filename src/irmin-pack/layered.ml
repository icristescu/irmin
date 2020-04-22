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

  module L : Pack.S

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

  val copy :
    [ `Read ] t ->
    dst:[ `Read | `Write ] L.t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t

  val check_and_copy :
    [ `Read ] t ->
    dst:[ `Read | `Write ] L.t ->
    aux:(value -> unit Lwt.t) ->
    string ->
    key ->
    unit Lwt.t

  val batch_lower : 'a t -> ([ `Read | `Write ] L.t -> 'b Lwt.t) -> 'b Lwt.t

  val mem_lower : 'a t -> key -> bool Lwt.t

  val upper : 'a t -> 'a U.t

  val lower : 'a t -> [ `Read ] L.t
end

module type CA = sig
  include Pack.S

  module Key : Irmin.Hash.TYPED with type t = key and type value = value
end

exception Copy_error of string

module Copy
    (Key : Irmin.Hash.S)
    (SRC : Pack.S with type key = Key.t)
    (DST : Pack.S with type key = SRC.key and type value = SRC.value) =
struct
  let add_to_dst name add dk (k, v) =
    add v >>= fun k' ->
    if not (Irmin.Type.equal dk k k') then
      Fmt.kstrf
        (fun x -> Lwt.fail (Copy_error x))
        "%s import error: expected %a, got %a" name
        Irmin.Type.(pp dk)
        k
        Irmin.Type.(pp dk)
        k'
    else Lwt.return_unit

  let already_in_dst ~dst k =
    DST.mem dst k >|= function
    | true ->
        Log.debug (fun l -> l "already in dst %a" (Irmin.Type.pp Key.t) k);
        true
    | false -> false

  let copy ~src ~dst ~aux str k =
    Log.debug (fun l -> l "copy %s %a" str (Irmin.Type.pp Key.t) k);
    SRC.find src k >>= function
    | None -> Lwt.return_unit
    | Some v -> aux v >>= fun () -> add_to_dst str (DST.add dst) Key.t (k, v)

  let check_and_copy ~src ~dst ~aux str k =
    already_in_dst ~dst k >>= function
    | true -> Lwt.return_unit
    | false -> copy ~src ~dst ~aux str k
end

module Content_addressable
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (S : Pack.S with type index = Index.t and type key = H.t) :
  LAYERED_S
    with type key = S.key
     and type value = S.value
     and type index = S.index
     and module U = S
     and type L.key = S.key
     and type L.value = S.value = struct
  type index = S.index

  type key = S.key

  type value = S.value

  module U = S
  module L = S

  type 'a upper = 'a U.t

  type 'a t = { upper : 'a upper; lower : [ `Read ] L.t }

  let batch_lower t f = L.batch t.lower f

  let mem_lower t k = L.mem t.lower k

  let v upper ?fresh ?readonly ?lru_size ~index root =
    L.v ?fresh ?readonly ?lru_size ~index root >|= fun lower -> { upper; lower }

  let add t = U.add t.upper

  let unsafe_add t = U.unsafe_add t.upper

  let unsafe_append t k v = U.unsafe_append t.upper k v

  let find t k =
    U.find t.upper k >>= function
    | None -> L.find t.lower k
    | Some v -> Lwt.return_some v

  let unsafe_find t k =
    match U.unsafe_find t.upper k with
    | None -> L.unsafe_find t.lower k
    | Some v -> Some v

  let mem t k =
    U.mem t.upper k >>= function
    | true -> Lwt.return_true
    | false -> L.mem t.lower k

  let unsafe_mem t k = U.unsafe_mem t.upper k || L.unsafe_mem t.lower k

  let project t upper = { t with upper }

  let batch () = failwith "don't call this function"

  let sync t =
    U.sync t.upper;
    L.sync t.lower

  let close t = U.close t.upper >>= fun () -> L.close t.lower

  type integrity_error = U.integrity_error

  let integrity_check ~offset ~length k t =
    let upper = t.upper in
    let lower = t.lower in
    match
      ( U.integrity_check ~offset ~length k upper,
        L.integrity_check ~offset ~length k lower )
    with
    | Ok (), Ok () -> Ok ()
    | Error `Wrong_hash, _ | _, Error `Wrong_hash -> Error `Wrong_hash
    | Error `Absent_value, _ | _, Error `Absent_value -> Error `Absent_value

  let layer_id t k =
    U.mem t.upper k >>= function
    | true -> Lwt.return 1
    | false -> (
        L.mem t.lower k >|= function true -> 2 | false -> raise Not_found )

  module Copy = Copy (H) (U) (S)

  let check_and_copy t ~dst ~aux str k =
    Copy.check_and_copy ~src:t.upper ~dst ~aux str k

  let copy t ~dst ~aux str k = Copy.copy ~src:t.upper ~dst ~aux str k

  let upper t = t.upper

  let lower t = t.lower
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
       and type U.key = key
       and type L.key = key
       and type U.value = V.t
       and type L.value = V.t
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
    include Content_addressable (H) (Index) (Upper)
  end
end

module type AW = sig
  include Irmin.ATOMIC_WRITE_STORE

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end

module Atomic_write (K : Irmin.Branch.S) (A : AW with type key = K.t) : sig
  include AW with type key = A.key and type value = A.value

  val v : A.t -> ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t

  val copy : t -> (value -> bool Lwt.t) -> unit Lwt.t
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

  (** Do not copy branches that point to commits not copied. *)
  let copy t commit_exists_lower =
    U.list t.upper >>= fun branches ->
    Lwt_list.iter_p
      (fun branch ->
        U.find t.upper branch >>= function
        | None -> Lwt.fail_with "branch not found in previous upper"
        | Some hash -> (
            commit_exists_lower hash >>= function
            | true ->
                Log.debug (fun l ->
                    l "[branches] copy to lower %a" (Irmin.Type.pp K.t) branch);
                A.set t.lower branch hash
            | false -> Lwt.return_unit ))
      branches
end
