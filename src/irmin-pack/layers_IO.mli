module type S = sig
  type t

  val v : string -> t

  val close : t -> unit

  val read_flip : t -> bool

  val write_flip : bool -> t -> unit
end

module Unix : S
