module Make (IO : IO.S) : sig
  type t

  val v : file:string -> default:bool -> t Lwt.t
  (** [v ~file ~default] creates a new flip, both on disk and in memory with the
      existing value on disk, or [default] if the file doesn't exist. *)

  val get : ?force:bool -> t -> bool Lwt.t
  (** [get t] is the value held in the flip [t]. If [force] is [true], this will
      check the value on the disk. Default to [false]. *)

  val set : t -> bool -> unit Lwt.t
  (** [set t b] sets the value of the flip [t] to [b], both on disk and in
      memory. *)

  val flip : t -> unit Lwt.t
  (** [flip t] is [set t (not (get t))]. *)

  val close : t -> unit Lwt.t
  (** Closes the file descriptor. *)
end
