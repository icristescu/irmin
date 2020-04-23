let src = Logs.Src.create "irmin.layers.io" ~doc:"IO for irmin-layers"

module Log = (val Logs.src_log src : Logs.LOG)

module type S = sig
  type t

  val v : string -> t

  val close : t -> unit

  val read_flip : t -> bool

  val write_flip : bool -> t -> unit
end

module Unix : S = struct
  type t = { file : string; fd : Unix.file_descr }

  let write ~offset t buf =
    let off = Unix.lseek t.fd 0 Unix.SEEK_SET in
    assert (off = offset);
    let len = Bytes.length buf in
    let n = Unix.write t.fd buf 0 len in
    assert (n = len)

  let read ~offset t buf =
    let off = Unix.lseek t.fd 0 Unix.SEEK_SET in
    assert (off = offset);
    let len = Bytes.length buf in
    let n = Unix.read t.fd buf 0 len in
    assert (n = len)

  let close t = Unix.close t.fd

  let write_flip flip t =
    let buf =
      if flip then Bytes.make 1 (char_of_int 1)
      else Bytes.make 1 (char_of_int 0)
    in
    write ~offset:0 t buf

  let read_flip t =
    let buf = Bytes.create 1 in
    read ~offset:0 t buf;
    let ch = Bytes.get buf 0 in
    match int_of_char ch with
    | 0 -> false
    | 1 -> true
    | d -> failwith ("corrupted flip file " ^ string_of_int d)

  let v file =
    match Sys.file_exists file with
    | false ->
        let fd = Unix.openfile file Unix.[ O_CREAT; O_RDWR; O_CLOEXEC ] 0o644 in
        let t = { file; fd } in
        write_flip true t;
        t
    | true ->
        let fd = Unix.openfile file Unix.[ O_EXCL; O_RDWR; O_CLOEXEC ] 0o644 in
        { file; fd }
end
