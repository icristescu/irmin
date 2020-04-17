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

open Lwt.Infix

let src = Logs.Src.create "irmin.layered" ~doc:"Irmin layered store"

module Log = (val Logs.src_log src : Logs.LOG)

module type CA = sig
  include Irmin.CONTENT_ADDRESSABLE_STORE

  module Key : Irmin.Hash.TYPED with type t = key and type value = value
end

exception Copy_error of string

module Copy
    (SRC : Irmin.CONTENT_ADDRESSABLE_STORE)
    (DST : CA with type key = SRC.key and type value = SRC.value) =
struct
  let pause pause_copy =
    match pause_copy with
    | None -> Lwt.return_unit
    | Some pause_copy ->
        let current = Stats.get_current_freeze () in
        if
          (current.contents + current.nodes + current.commits) mod pause_copy
          = 0
        then (
          Log.debug (fun l -> l "pausing copy thread...");
          Lwt.pause () )
        else Lwt.return_unit

  let add_to_dst name add dk (k, v) pause_copy =
    pause pause_copy >>= fun () ->
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
        Log.debug (fun l -> l "already in dst %a" (Irmin.Type.pp DST.Key.t) k);
        true
    | false -> false

  let copy ~src ~dst ~aux str k pause_copy =
    Log.debug (fun l -> l "copy %s %a" str (Irmin.Type.pp DST.Key.t) k);
    SRC.find src k >>= function
    | None -> Lwt.return_unit
    | Some v ->
        aux v >>= fun () ->
        add_to_dst str (DST.add dst) DST.Key.t (k, v) pause_copy

  let check_and_copy ~src ~dst ~aux str k pause_copy =
    already_in_dst ~dst k >>= function
    | true -> Lwt.return_false
    | false -> copy ~src ~dst ~aux str k pause_copy >|= fun () -> true
end

module Content_addressable
    (K : Irmin.Hash.S)
    (V : Irmin.Type.S)
    (L : CA with type key = K.t and type value = V.t)
    (U : CA with type key = K.t and type value = V.t) =
struct
  type key = K.t

  type value = V.t

  type 'a t = {
    lower : [ `Read ] L.t;
    mutable flip : bool;
    uppers : 'a U.t * 'a U.t;
    lock : Lwt_mutex.t;
    pause_copy : int option;
    pause_add : int option;
  }

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  module CopyUpper = Copy (U) (U)
  module CopyLower = Copy (U) (L)

  type 'a layer_type =
    | Upper : [ `Read | `Write ] U.t layer_type
    | Lower : [ `Read | `Write ] L.t layer_type

  let already_in_dst : type l. l layer_type * l -> key -> bool Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> CopyLower.already_in_dst ~dst
    | Upper -> CopyUpper.already_in_dst ~dst

  let stats str = function
    | true -> (
        match str with
        | "Contents" -> Stats.copy_contents ()
        | "Node" -> Stats.copy_nodes ()
        | "Commit" -> Stats.copy_commits ()
        | _ -> failwith "unexpected type in stats" )
    | false -> ()

  let check_and_copy_to_lower t ~dst ~aux str k =
    CopyLower.check_and_copy ~src:(previous_upper t) ~dst ~aux str k
      t.pause_copy
    >|= stats str

  let check_and_copy_to_current t ~dst ~aux str (k : key) =
    CopyUpper.check_and_copy ~src:(previous_upper t) ~dst ~aux str k
      t.pause_copy
    >|= stats str

  let check_and_copy :
      type l.
      l layer_type * l ->
      [ `Read ] t ->
      aux:(value -> unit Lwt.t) ->
      string ->
      key ->
      unit Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> check_and_copy_to_lower ~dst
    | Upper -> check_and_copy_to_current ~dst

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  (* RO instances do not know which of the two uppers is in use by RW, so find
     (and mem) has to look in both uppers. *)
  let find t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None ->
            Log.debug (fun l -> l "[branches] find in lower");
            L.find t.lower k )

  let mem t k =
    let current = current_upper t in
    let previous = previous_upper t in
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false -> L.mem t.lower k )

  let pause t =
    match t.pause_add with
    | None -> Lwt.return_unit
    | Some pause_add ->
        let adds = Stats.get_adds () in
        if adds mod pause_add = 0 then (
          Log.debug (fun l -> l "pausing main thread...");
          Lwt.pause () )
        else Lwt.return_unit

  let add t v =
    pause t >>= fun () ->
    Lwt_mutex.with_lock t.lock (fun () ->
        Log.debug (fun l -> l "add in %s" (log_current_upper t));
        let upper = current_upper t in
        Stats.add ();
        U.add upper v)

  let unsafe_add t k v =
    pause t >>= fun () ->
    Lwt_mutex.with_lock t.lock (fun () ->
        Log.debug (fun l -> l "unsafe_add in %s" (log_current_upper t));
        let upper = current_upper t in
        Stats.add ();
        U.unsafe_add upper k v)

  let v upper second_upper lower flip lock pause_copy pause_add =
    { lower; flip; uppers = (upper, second_upper); lock; pause_copy; pause_add }

  let project upper second_upper t = { t with uppers = (upper, second_upper) }

  let layer_id t k =
    Log.debug (fun l -> l "layer_id of %a" (Irmin.Type.pp K.t) k);
    U.mem (fst t.uppers) k >>= function
    | true -> Lwt.return 1
    | false -> (
        U.mem (snd t.uppers) k >>= function
        | true -> Lwt.return 0
        | false -> (
            L.mem t.lower k >>= function
            | true -> Lwt.return 2
            | false -> raise Not_found ) )

  (* clear the upper that is not in use *)
  let clear_upper t =
    Log.debug (fun l -> l "clear %s" (log_previous_upper t));
    U.clear (previous_upper t)

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () -> L.clear t.lower

  let flip_upper t =
    Log.debug (fun l -> l "flip_upper to %s" (log_previous_upper t));
    t.flip <- not t.flip
end

module Atomic_write
    (K : Irmin.Type.S)
    (V : Irmin.Hash.S)
    (L : Irmin.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t)
    (U : Irmin.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t) =
struct
  type t = {
    lower : L.t;
    mutable flip : bool;
    uppers : U.t * U.t;
    lock : Lwt_mutex.t;
  }

  type key = K.t

  type value = V.t

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  (* RO instances do not know which of the two uppers is in use by RW, so find
     (and mem) has to look in both uppers. TODO if branch exists in both
     uppers, then we have to check which upper is in use by RW. *)
  let mem t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] mem in %s" (log_current_upper t));
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        Log.debug (fun l -> l "[branches] mem in%s" (log_previous_upper t));
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false ->
            Log.debug (fun l -> l "[branches] mem in lower");
            L.mem t.lower k )

  let find t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "[branches] find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None ->
            Log.debug (fun l -> l "[branches] find in lower");
            L.find t.lower k )

  let unsafe_set t k v =
    Log.debug (fun l ->
        l "unsafe set %a %a in %s" (Irmin.Type.pp K.t) k (Irmin.Type.pp V.t) v
          (log_current_upper t));
    let upper = current_upper t in
    U.set upper k v

  let set t k v = Lwt_mutex.with_lock t.lock (fun () -> unsafe_set t k v)

  (* we have to copy back into upper (or async_upper) the branch against we want to do
     test and set *)
  let unsafe_test_and_set t k ~test ~set =
    let current = current_upper t in
    let previous = previous_upper t in
    let find_in_lower () =
      L.find t.lower k >>= function
      | None -> U.test_and_set current k ~test:None ~set
      | Some v ->
          U.set current k v >>= fun () -> U.test_and_set current k ~test ~set
    in
    U.mem current k >>= function
    | true -> U.test_and_set current k ~test ~set
    | false -> (
        U.find previous k >>= function
        | None -> find_in_lower ()
        | Some v ->
            U.set current k v >>= fun () -> U.test_and_set current k ~test ~set
        )

  let test_and_set t k ~test ~set =
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_test_and_set t k ~test ~set)

  let remove t k =
    U.remove (fst t.uppers) k >>= fun () ->
    U.remove (snd t.uppers) k >>= fun () -> L.remove t.lower k

  let list t =
    U.list (fst t.uppers) >>= fun upper1 ->
    U.list (snd t.uppers) >>= fun upper2 ->
    L.list t.lower >|= fun lower ->
    List.fold_left
      (fun acc b -> if List.mem b acc then acc else b :: acc)
      lower (upper1 @ upper2)

  type watch = U.watch

  let watch t = U.watch (current_upper t)

  let watch_key t = U.watch_key (current_upper t)

  let unwatch t = U.unwatch (current_upper t)

  let close t =
    U.close (fst t.uppers) >>= fun () ->
    U.close (snd t.uppers) >>= fun () -> L.close t.lower

  (* clear the upper that is not in use *)
  let clear_upper t = U.clear (previous_upper t)

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () -> L.clear t.lower

  let v upper second_upper lower flip lock =
    { lower; flip; uppers = (upper, second_upper); lock }

  (** Do not copy branches that point to commits not copied. *)
  let copy t commit_exists_lower commit_exists_upper =
    let previous = previous_upper t in
    let current = current_upper t in
    U.list previous >>= fun branches ->
    Lwt_list.iter_p
      (fun branch ->
        U.find previous branch >>= function
        | None -> Lwt.fail_with "branch not found in previous upper"
        | Some hash -> (
            (commit_exists_lower hash >>= function
             | true ->
                 Log.debug (fun l ->
                     l "[branches] copy to lower %a" (Irmin.Type.pp K.t) branch);
                 Stats.copy_branches ();
                 L.set t.lower branch hash
             | false -> Lwt.return_unit)
            >>= fun () ->
            commit_exists_upper hash >>= function
            | true ->
                Log.debug (fun l ->
                    l "[branches] copy to current %a" (Irmin.Type.pp K.t) branch);
                Stats.copy_branches ();
                U.set current branch hash
            | false -> Lwt.return_unit ))
      branches

  let flip_upper t =
    Log.debug (fun l -> l "[branches] flip to %s" (log_previous_upper t));
    t.flip <- not t.flip
end
