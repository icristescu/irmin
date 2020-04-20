module Make (IO : IO.S) = struct
  exception Closed

  exception Corrupted

  type t = {
    mutable mem : bool;
    disk : IO.t;
    mutable closed : bool;
    lock : Lwt_mutex.t;
  }

  let raw_get io =
    let buf = Bytes.create 1 in
    IO.read io buf;
    Bytes.get buf 0 |> function
    | '0' -> false
    | '1' -> true
    | _ -> raise Corrupted

  let unsafe_get t force =
    if t.closed then raise Closed;
    if force then (
      let bit = raw_get t.disk in
      t.mem <- bit;
      bit )
    else t.mem

  let get ?(force = false) t =
    Lwt_mutex.with_lock t.lock (fun () -> Lwt.return (unsafe_get t force))

  let unsafe_set t b =
    if t.closed then raise Closed;
    (if b then "1" else "0") |> Bytes.of_string |> IO.write t.disk;
    t.mem <- b

  let set t b =
    Lwt_mutex.with_lock t.lock (fun () -> Lwt.return (unsafe_set t b))

  let flip t =
    Lwt_mutex.with_lock t.lock (fun () -> Lwt.return (unsafe_set t (not t.mem)))

  let close t =
    Lwt_mutex.with_lock t.lock (fun () -> Lwt.return (t.closed <- true))

  let v ~file ~default =
    let disk = (if default then "1" else "0") |> Bytes.of_string |> IO.v file in
    let mem = raw_get disk in
    let closed = false in
    let lock = Lwt_mutex.create () in
    Lwt.return { mem; disk; closed; lock }
end
