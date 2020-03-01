open Lwt.Infix

let root = Filename.concat "_build" "test-concurrent"

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

let report () =
  Logs_threaded.enable ();
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (reporter ())

let src = Logs.Src.create "tests.unix.concurrent" ~doc:"Tests"

module Log = (val Logs.src_log src : Logs.LOG)

let index_log_size = Some 1_000

let parent_pid = Unix.getpid ()

module Conf = struct
  let entries = 32

  let stable_hash = 256

  let lower_root = "_lower"

  let second_upper_root = "1"

  let keep_max = true
end

module Hash = Irmin.Hash.SHA1
module S =
  Irmin_pack.Make (Conf) (Irmin.Metadata.None) (Irmin.Contents.String)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Hash)

let config ?(readonly = false) ?(fresh = true) root =
  Irmin_pack.config ~readonly ?index_log_size ~fresh root

let random_char () = char_of_int (33 + Random.int 94)

let random_string string_size =
  String.init string_size (fun _i -> random_char ())

let info () = Irmin.Info.empty

let wait pid =
  Lwt_unix.waitpid [ Unix.WUNTRACED ] pid >>= fun (pid', status) ->
  if pid <> pid' then
    Alcotest.failf "I'm %d, expecting child %d, but got %d instead"
      (Unix.getpid ()) pid pid';
  match status with
  | Unix.WEXITED 0 ->
      Log.debug (fun l -> l "Child %d finished work" pid);
      Lwt.return_unit
  | Unix.WSTOPPED s ->
      Alcotest.failf "Child %d died unexpectedly stopped by %d" pid s
  | Unix.WEXITED s ->
      Alcotest.failf "Child %d died unexpectedly exited with %d" pid s
  | Unix.WSIGNALED s ->
      Alcotest.failf "Child %d died unexpectedly signaled by %d" pid s

let worker () =
  Log.debug (fun l -> l "worker started");
  S.Repo.v (config ~readonly:true ~fresh:false root) >>= fun ro ->
  S.master ro >>= fun t ->
  S.Head.get t >>= fun c ->
  (S.Commit.of_hash ro (S.Commit.hash c) >>= function
   | None -> Alcotest.fail "no hash found in repo"
   | Some commit ->
       let tree = S.Commit.tree commit in
       S.Tree.find tree [ "a"; "b" ] >|= fun novembre ->
       Alcotest.(check (option string)) "nov" (Some "Novembre") novembre)
  >>= fun () -> S.Repo.close ro

let test _switch () =
  match Lwt_unix.fork () with
  | 0 -> worker () >|= fun () -> exit 0
  | pid ->
      S.Repo.v (config ~readonly:false root) >>= fun rw ->
      S.master rw >>= fun t ->
      S.Tree.add S.Tree.empty [ "a"; "b" ] "Novembre" >>= fun tree ->
      S.set_tree_exn ~parents:[] ~info t [] tree >>= fun () ->
      Log.debug (fun l -> l "parent %d waits for worker %d" parent_pid pid);
      wait pid >>= fun () -> S.Repo.close rw

let () =
  if Unix.getpid () = parent_pid then (
    report ();
    Lwt_main.run (test 1 ()) )

(*let tests = ("tests", [ Alcotest_lwt.test_case "pack" `Quick test ]) in
  Alcotest.run "concurrent tests" [ tests ] )*)
