module type S = sig
  val entries : int

  val stable_hash : int
end

val fresh_key : bool Irmin.Private.Conf.key

val lru_size_key : int Irmin.Private.Conf.key

val index_log_size_key : int Irmin.Private.Conf.key

val readonly_key : bool Irmin.Private.Conf.key

val root_key : string option Irmin.Private.Conf.key

val fresh : Irmin.Private.Conf.t -> bool

val lru_size : Irmin.Private.Conf.t -> int

val index_log_size : Irmin.Private.Conf.t -> int

val readonly : Irmin.Private.Conf.t -> bool

type merge_throttle = [ `Block_writes | `Overcommit_memory ] [@@deriving irmin]

val merge_throttle_key : merge_throttle Irmin.Private.Conf.key

val merge_throttle : Irmin.Private.Conf.t -> merge_throttle

type freeze_throttle = [ merge_throttle | `Cancel_existing ] [@@deriving irmin]

val freeze_throttle_key : freeze_throttle Irmin.Private.Conf.key

val freeze_throttle : Irmin.Private.Conf.t -> freeze_throttle

val root : Irmin.Private.Conf.t -> string

val v :
  ?fresh:bool ->
  ?readonly:bool ->
  ?lru_size:int ->
  ?index_log_size:int ->
  ?merge_throttle:merge_throttle ->
  ?freeze_throttle:freeze_throttle ->
  string ->
  Irmin.config
