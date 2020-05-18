open Lwt.Infix
open Common

let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let ppf = match level with Logs.App -> Fmt.stdout | _ -> Fmt.stderr in
    let with_stamp h _tags k fmt =
      let dt = Unix.gettimeofday () in
      Fmt.kpf k ppf
        ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        prefix dt
        Fmt.(styled `Magenta string)
        (Logs.Src.name src) Logs_fmt.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  { Logs.report }

let _file_reporter () =
  let buf_fmt ~like =
    let b = Buffer.create 512 in
    ( Fmt.with_buffer ~like b,
      fun () ->
        let m = Buffer.contents b in
        Buffer.reset b;
        m )
  in
  let fd =
    Unix.openfile "my_file.log" Unix.[ O_CREAT; O_WRONLY; O_APPEND ] 0o644
  in
  let report src level ~over k msgf =
    let app, app_flush = buf_fmt ~like:Fmt.stdout in
    let dst, dst_flush = buf_fmt ~like:Fmt.stderr in
    let reporter = Logs_fmt.reporter ~app ~dst () in
    let k () =
      let buf = Bytes.unsafe_of_string (app_flush ()) in
      let _ = Unix.write fd buf 0 (Bytes.length buf) in
      let buf = Bytes.unsafe_of_string (dst_flush ()) in
      let _ = Unix.write fd buf 0 (Bytes.length buf) in
      over ();
      k ()
    in
    reporter.Logs.report src level ~over:(fun () -> ()) k msgf
  in
  { Logs.report }

let index_src =
  let open Metrics in
  let open Index.Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "bytes_read" t.bytes_read;
        int "bytes_written" t.bytes_written;
        int "merge" t.nb_merge;
        int "replace" t.nb_replace;
      ]
  in
  Src.v "bench_index" ~tags ~data

let pack_src =
  let open Metrics in
  let open Irmin_pack.Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "find" t.finds;
        int "appended_hashes" t.appended_hashes;
        int "appended_offsets" t.appended_offsets;
      ]
  in
  Src.v "bench_irmin" ~tags ~data

let layers_src =
  let open Metrics in
  let string_of_ints ls = String.concat "," (List.map string_of_int ls) in
  let int_list k v =
    field ~doc:"List of int" k (Other (Fmt.of_to_string string_of_ints)) v
  in
  let open Irmin_layers.Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "freezes" t.nb_freeze;
        int_list "copied_contents" t.copied_contents;
        int_list "copied_nodes" t.copied_nodes;
        int_list "copied_commits" t.copied_commits;
        int_list "copied_branches" t.copied_branches;
      ]
  in
  Src.v "bench_layers" ~tags ~data

let add_metrics () =
  let no_tags x = x in
  let index_stats = Index.Stats.get () in
  let pack_stats = Irmin_pack.Stats.get () in
  let layers_stats = Irmin_layers.Stats.get () in
  Metrics_lwt.add index_src no_tags (fun m -> Lwt.return (m index_stats))
  >>= fun () ->
  Metrics_lwt.add pack_src no_tags (fun m -> Lwt.return (m pack_stats))
  >>= fun () ->
  Metrics_lwt.add layers_src no_tags (fun m -> Lwt.return (m layers_stats))

let get_fds fd_file =
  let pid = string_of_int (Unix.getpid ()) in
  let lsof_command = "lsof -a -s -p " ^ pid ^ " > " ^ fd_file in
  match Unix.system lsof_command with
  | Unix.WEXITED 0 -> ()
  | Unix.WEXITED _ ->
      failwith "failing `lsof` command. Is `lsof` installed on your system?"
  | Unix.WSIGNALED _ | Unix.WSTOPPED _ ->
      failwith "`lsof` command was interrupted"

let reset_stats () =
  Index.Stats.reset_stats ();
  Irmin_pack.Stats.reset_stats ();
  Irmin_layers.Stats.reset_stats ()

open Cmdliner

let ncommits =
  let doc = Arg.info ~doc:"Number of commits per batch." [ "n"; "ncommits" ] in
  Arg.(value @@ opt int 50 doc)

let nbatches =
  let doc = Arg.info ~doc:"Number of batches." [ "b"; "nbatches" ] in
  Arg.(value @@ opt int 5 doc)

let depth =
  let doc = Arg.info ~doc:"Depth of a commit's tree." [ "d"; "depth" ] in
  Arg.(value @@ opt int 1000 doc)

let clear =
  let doc =
    Arg.info ~doc:"Clear the tree after each commit." [ "c"; "clear" ]
  in
  Arg.(value @@ opt bool false doc)

let metrics_flag =
  let doc =
    Arg.info ~doc:"Use Metrics; note that it has an impact on performance"
      [ "m"; "metrics" ]
  in
  Arg.(value @@ opt bool false doc)

let squash =
  let doc = Arg.info ~doc:"Freeze with squash." [ "s"; "squash" ] in
  Arg.(value @@ opt bool false doc)

let keep_max =
  let doc = Arg.info ~doc:"Freeze with keep_max." [ "k"; "keep_max" ] in
  Arg.(value @@ opt bool true doc)

let reader =
  let doc = Arg.info ~doc:"Benchmark RO reads." [ "r"; "reader" ] in
  Arg.(value @@ opt bool false doc)

let no_freeze =
  let doc = Arg.info ~doc:"Without freeze." [ "f"; "no_freeze" ] in
  Arg.(value @@ opt bool false doc)

let pause_copy =
  let doc =
    Arg.info ~doc:"Pause worker thread every n copies." [ "p"; "pause_copy" ]
  in
  Arg.(value @@ opt int 0 doc)

let pause_add =
  let doc =
    Arg.info ~doc:"Pause main thread every n adds." [ "a"; "pause_add" ]
  in
  Arg.(value @@ opt int 0 doc)

type config = {
  ncommits : int;
  nbatches : int;
  depth : int;
  root : string;
  clear : bool;
  with_metrics : bool;
  squash : bool;
  keep_max : bool;
  reader : bool;
  no_freeze : bool;
  pause_copy : int;
  pause_add : int;
}

let index_log_size = Some 1_000

let long_random_blob () = random_string 100

let long_random_key () = random_string 100

let keys : string list list ref = ref []

module Conf = struct
  let entries = 32

  let stable_hash = 256

  let keep_max = true
end

module Hash = Irmin.Hash.SHA1
module Store =
  Irmin_pack.Make_layered (Conf) (Irmin.Metadata.None) (Irmin.Contents.String)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Hash)

module FSHelper = struct
  let file f =
    try (Unix.stat f).st_size with Unix.Unix_error (Unix.ENOENT, _, _) -> 0

  let dict root = file (Filename.concat root "store.dict") / 1024 / 1024

  let pack root = file (Filename.concat root "store.pack") / 1024 / 1024

  let branches root = file (Filename.concat root "store.branches") / 1024 / 1024

  let index root =
    let index_dir = Filename.concat root "index" in
    let a = file (Filename.concat index_dir "data") in
    let b = file (Filename.concat index_dir "log") in
    let c = file (Filename.concat index_dir "log_async") in
    (a + b + c) / 1024 / 1024

  let size root = dict root + pack root + index root

  let print_size root =
    let dt = Unix.gettimeofday () in
    let upper1 = Filename.concat root "upper1" in
    let upper0 = Filename.concat root "upper0" in
    let lower = Filename.concat root "lower" in
    Fmt.epr "%+04.0fus: upper1 = %d M, upper0 = %d M, lower = %d M\n%!" dt
      (size upper1) (size upper0) (size lower)
end

let configure_store ?(readonly = false) ?(fresh = true)
    ?(keep_max = Conf.keep_max) ?(pause_copy = 0) ?(pause_add = 0) root =
  let conf = Irmin_pack.config ~readonly ?index_log_size ~fresh root in
  Irmin_pack.config_layers ~conf ~keep_max ~pause_copy ~pause_add ()

let init config =
  rm_dir config.root;
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Logs.App);
  Logs.set_reporter (reporter ());
  reset_stats ();
  if config.with_metrics then (
    Metrics.enable_all ();
    Metrics_gnuplot.set_reporter ();
    Metrics_unix.monitor_gc 0.1 )

let info () =
  let date = Int64.of_float (Unix.gettimeofday ()) in
  let author = Printf.sprintf "TESTS" in
  Irmin.Info.v ~date ~author "commit "

let key conf =
  let rec go i acc =
    if i = conf.depth then acc
    else
      let k = long_random_key () in
      go (i + 1) (k :: acc)
  in
  go 0 []

let generate_keys conf =
  let rec go i =
    if i = conf.ncommits then ()
    else
      let k = key conf in
      keys := k :: !keys;
      go (i + 1)
  in
  go 0

let add_tree ?i conf tree =
  let k = match i with None -> key conf | Some i -> List.nth !keys i in
  let tree = if conf.clear then Store.Tree.empty else tree in
  Store.Tree.add tree k (long_random_blob ())

let init_commit repo =
  Store.Commit.v repo ~info:(info ()) ~parents:[] Store.Tree.empty

let checkout_and_commit ?i config repo c =
  Store.Commit.of_hash repo c >>= function
  | None -> Lwt.fail_with "commit not found"
  | Some commit ->
      let tree = Store.Commit.tree commit in
      add_tree ?i config tree >>= fun tree ->
      Store.Commit.v repo ~info:(info ()) ~parents:[ c ] tree

let with_timer f =
  let t0 = Sys.time () in
  f () >|= fun a ->
  let t1 = Sys.time () -. t0 in
  (t1, a)

let write_batch ?(generated_keys = false) config repo init_commit =
  let rec go (times, c) i =
    if i = config.ncommits then Lwt.return (List.rev times, c)
    else
      let hash_c = Store.Commit.hash c in
      let commit =
        if generated_keys then checkout_and_commit ~i config
        else checkout_and_commit config
      in
      with_timer (fun () -> commit repo hash_c) >>= fun (time, c') ->
      go (time :: times, c') (i + 1)
  in
  go ([], init_commit) 0

let write_keys config repo c = write_batch ~generated_keys:true config repo c

let freeze config repo h =
  if config.no_freeze then Lwt.return_unit
  else Store.freeze ~squash:config.squash ~max:[ h ] repo

let read_batch store =
  let find key =
    Store.find store key >|= function
    | None -> failwith "RO does not find key"
    | Some _ -> ()
  in
  Lwt_list.fold_left_s
    (fun acc key ->
      with_timer (fun () -> find key) >|= fun (time, ()) -> time +. acc)
    0.0 !keys

let run_batches config repo init_commit =
  let rec go c i =
    if i = config.nbatches then Lwt.return c
    else
      write_batch config repo c >>= fun (_times, c) ->
      (* Fmt.epr "batch = %d\n%!" i; *)
      (* List.iter (fun t -> Fmt.epr "%f\n%!" t) times; *)
      (if config.with_metrics then add_metrics () else Lwt.return_unit)
      >>= fun () ->
      with_timer (fun () -> freeze config repo c) >>= fun (_t, ()) ->
      let stats = Irmin_layers.Stats.get () in
      Fmt.epr "freeze pause_add = %d pause_copy = %d\n%!" stats.pause_add
        stats.pause_copy;
      go c (i + 1)
  in
  go init_commit 0

let rw config =
  let conf =
    configure_store config.root ~keep_max:config.keep_max
      ~pause_copy:config.pause_copy ~pause_add:config.pause_add
  in
  Store.Repo.v conf >>= fun repo ->
  Store.master repo >|= fun store -> (repo, store)

let integrity_check config =
  let conf = configure_store config.root ~keep_max:config.keep_max in
  Store.Repo.v conf >>= fun repo ->
  ( match Store.integrity_check ~auto_repair:false repo with
  | Ok `No_error -> Fmt.epr "No errors detected by integrity check\n%!"
  | Error (`Corrupted n) -> Fmt.epr "Corrupted entries %d\n%!" n
  | _ -> Fmt.epr "unexpected" );
  Store.Repo.close repo

let worker_thread config r_pipe =
  let ro_conf = configure_store ~readonly:true ~fresh:false config.root in
  ignore (Concurrent.read r_pipe);

  let rec read i =
    if i = 20 then Lwt.return_unit
    else
      let sec = 0.2 *. float_of_int i in
      Lwt_unix.sleep sec >>= fun () ->
      Store.Repo.v ro_conf >>= fun repo ->
      Store.master repo >>= fun store ->
      read_batch store >>= fun times ->
      Store.Repo.close repo >>= fun () ->
      Fmt.epr "%f\n%!" times;
      read (i + 1)
  in
  read 0

let concurrent_reads config =
  generate_keys config;
  let read_worker, write_main = Unix.pipe () in
  match Lwt_unix.fork () with
  | 0 ->
      Printexc.record_backtrace true;
      Fmt_tty.setup_std_outputs ();
      Logs.set_level (Some Logs.App);
      Logs.set_reporter (reporter ());
      Logs.debug (fun l -> l "Worker %d created" (Unix.getpid ()));
      worker_thread config read_worker >|= fun () -> exit 0
  | pid ->
      init config;
      Logs.debug (fun l -> l "Main %d started" (Unix.getpid ()));
      rw config >>= fun (repo, _store) ->
      init_commit repo >>= fun c ->
      write_keys config repo c >>= fun (_times, c) ->
      Store.Branch.set repo "master" c >>= fun () ->
      Store.sync repo;
      (* List.iter (fun t -> Fmt.epr "%f\n%!" t) times; *)
      with_timer (fun () -> freeze config repo c) >>= fun (_t, ()) ->
      Fmt.epr "freeze \n%!";
      Concurrent.write write_main;
      run_batches config repo c >>= fun _ ->
      Concurrent.wait pid >>= fun () ->
      Unix.close read_worker;
      Unix.close write_main;
      Store.Repo.close repo

let run config =
  ( if config.reader then concurrent_reads config
  else (
    init config;
    rw config >>= fun (repo, _store) ->
    init_commit repo >>= fun c ->
    run_batches config repo c >>= fun _ ->
    FSHelper.print_size config.root;
    Store.Repo.close repo >>= fun () -> integrity_check config ) )
  >|= fun () ->
  Fmt.epr "After freeze thread finished : ";
  FSHelper.print_size config.root;
  if config.with_metrics then add_metrics () else Lwt.return_unit

let main ncommits nbatches depth clear with_metrics squash keep_max reader
    no_freeze pause_copy pause_add =
  let config =
    {
      ncommits;
      nbatches;
      depth;
      root = "test-bench";
      clear;
      with_metrics;
      squash;
      keep_max;
      reader;
      no_freeze;
      pause_copy;
      pause_add;
    }
  in
  Fmt.epr
    "Benchmarking ./%s with depth = %d, ncommits/batch = %d, nbatches = %d, \
     clear = %b, metrics = %b, squash = %b, keep_max = %b, reader = %b \
     no_freeze = %b, pause_copy = %d, pause_add =%d \n\
     %!"
    config.root config.depth config.ncommits config.nbatches config.clear
    config.with_metrics config.squash config.keep_max config.reader
    config.no_freeze config.pause_copy config.pause_add;
  Lwt_main.run (run config)

let main_term =
  Term.(
    const main
    $ ncommits
    $ nbatches
    $ depth
    $ clear
    $ metrics_flag
    $ squash
    $ keep_max
    $ reader
    $ no_freeze
    $ pause_copy
    $ pause_add)

let () =
  let info = Term.info "Benchmarks for layered store" in
  Term.exit @@ Term.eval (main_term, info)
