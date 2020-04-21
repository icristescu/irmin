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
     and module U = S

module type AW = sig
  include Irmin.ATOMIC_WRITE_STORE

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end

module Atomic_write (A : AW) : sig
  include AW with type key = A.key and type value = A.value

  val v : A.t -> ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end

module type LAYERED_MAKER = sig
  type key

  type index

  (** Save multiple kind of values in the same pack file. Values will be
      distinguished using [V.magic], so they have to all be different. *)
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
  LAYERED_MAKER with type key = S.key and type index = S.index
