open Lwt.Infix
open Common

let src = Logs.Src.create "test" ~doc:"Irmin layered tests"

module Log = (val Logs.src_log src : Logs.LOG)

module Make_Layered (S : LAYERED_STORE) = struct
  module P = S.Private
  module Graph = Irmin.Private.Node.Graph (P.Node)
  module History = Irmin.Private.Commit.History (P.Commit)

  let v1 = "X1"

  let v2 = "X2"

  let b1 = "foo"

  let b2 = "bar/toto"

  let with_contents repo f = P.Repo.batch repo (fun t _ _ -> f t)

  let with_node repo f = P.Repo.batch repo (fun _ t _ -> f t)

  let with_commit repo f = P.Repo.batch repo (fun _ _ t -> f t)

  let with_info repo n f = with_commit repo (fun h -> f h ~info:(info n))

  let normal x = `Contents (x, S.Metadata.default)

  let h repo = P.Repo.commit_t repo

  let n repo = P.Repo.node_t repo

  let n1 ~repo =
    with_contents repo (fun t -> P.Contents.add t v1) >>= fun kv1 ->
    with_node repo (fun t -> Graph.v t [ ("x", normal kv1) ]) >>= fun kn1 ->
    with_node repo (fun t -> Graph.v t [ ("b", `Node kn1) ])

  let r1 ~repo =
    n1 ~repo >>= fun kn2 ->
    S.Tree.of_hash repo kn2 >>= function
    | None -> Alcotest.fail "r1"
    | Some tree -> S.Commit.v repo ~info:(info "r1") ~parents:[] (tree :> S.tree)

  let r2 ~repo =
    n1 ~repo >>= fun kn2 ->
    with_node repo (fun t -> Graph.v t [ ("a", `Node kn2) ]) >>= fun kn3 ->
    r1 ~repo >>= fun kr1 ->
    S.Tree.of_hash repo kn3 >>= function
    | None -> Alcotest.fail "r2"
    | Some t3 ->
        S.Commit.v repo ~info:(info "r2") ~parents:[ S.Commit.hash kr1 ]
          (t3 :> S.tree)

  let run x test =
    try
      Lwt_main.run
        ( x.init () >>= fun () ->
          S.Repo.v x.config >>= fun repo -> test repo >>= x.clean )
    with e ->
      Lwt_main.run (x.clean ());
      raise e

  let fail_with_none f msg =
    f >>= function None -> Alcotest.fail msg | Some c -> Lwt.return c

  (* add nodes (from Private.Graph), commits (from Private.History)
           commits:kr2 ---> kr1
                    |        |
            nodes :kt3 -b-> kt2 -a-> kt1 -x-> kv1
     freeze kr1; find nodes and commits; check that kr2 is deleted;
     reconstruct node kt3 and commit again kr2; freeze kr2 and check again*)
  let test_graph_and_history x () =
    let test repo =
      with_contents repo (fun t -> P.Contents.add t v1) >>= fun kv1 ->
      let check_val = check (T.option P.Commit.Val.t) in
      let check_key = check P.Commit.Key.t in
      let check_keys = checks P.Commit.Key.t in
      with_node repo (fun g -> Graph.v g [ ("x", normal kv1) ]) >>= fun kt1 ->
      with_node repo (fun g -> Graph.v g [ ("a", `Node kt1) ]) >>= fun kt2 ->
      with_node repo (fun g -> Graph.v g [ ("b", `Node kt2) ]) >>= fun kt3 ->
      with_info repo "commit kt2" (History.v ~node:kt2 ~parents:[])
      >>= fun (kr1, _) ->
      with_info repo "commit kt3" (History.v ~node:kt3 ~parents:[ kr1 ])
      >>= fun (kr2, _) ->
      P.Commit.find (h repo) kr1 >>= fun t1 ->
      fail_with_none (S.Commit.of_hash repo kr1) "of_hash commit"
      >>= fun commit ->
      S.freeze repo ~max:[ commit ] >>= fun () ->
      P.Commit.find (h repo) kr1 >>= fun t1' ->
      check_val "value of kr1 before and after freeze" t1 t1';
      Graph.find (n repo) kt2 [ "a" ] >>= fun kt1' ->
      check (T.option Graph.value_t) "kt2 -a-> kt1 before and after freeze"
        (Some (`Node kt1))
        kt1';
      Graph.find (n repo) kt3 [ "b" ] >>= fun kt2' ->
      check (T.option Graph.value_t) "kt3 -b-> kt2 deleted by freeze" None kt2';
      with_node repo (fun g -> Graph.v g [ ("b", `Node kt2) ]) >>= fun kt3 ->
      with_info repo "commit kt3" (History.v ~node:kt3 ~parents:[ kr1 ])
      >>= fun (kr2', _) ->
      check_key "commit kr2 after freeze" kr2 kr2';
      History.closure (h repo) ~min:[] ~max:[ kr1 ] >>= fun kr1s ->
      check_keys "closure over lower" [ kr1 ] kr1s;
      History.closure (h repo) ~min:[] ~max:[ kr2 ] >>= fun kr2s ->
      check_keys "closure over upper and lower" [ kr1; kr2 ] kr2s;
      P.Commit.find (h repo) kr2' >>= fun t2 ->
      fail_with_none (S.Commit.of_hash repo kr2') "of_hash commit"
      >>= fun commit ->
      S.freeze repo ~max:[ commit ] >>= fun () ->
      P.Commit.find (h repo) kr1 >>= fun t1' ->
      check_val "value of kr1 before and after snd freeze" t1 t1';
      P.Commit.find (h repo) kr2 >>= fun t2' ->
      check_val "value of kr2 before and after snd freeze" t2 t2';
      Graph.find (n repo) kt2 [ "a" ] >>= fun kt1' ->
      check (T.option Graph.value_t) "kt2 -a-> kt1 before and after snd freeze"
        (Some (`Node kt1))
        kt1';
      Graph.find (n repo) kt3 [ "b" ] >>= fun kt2' ->
      check (T.option Graph.value_t) "kt3 -b-> kt2 before and after snd freeze"
        (Some (`Node kt2))
        kt2';

      S.Repo.close repo
    in
    run x test

  (* test trees, trees from commits, temporary stores from commits *)
  let test_gc x () =
    let info = info "gc" in
    let check_val = check (T.option P.Commit.Val.t) in
    (*
      -> c0 -> c1 -> c2 |freeze|
                 \-> c3
    *)
    let tree1 repo =
      let tree = S.Tree.empty in
      S.Tree.add tree [ "c"; "b"; "a" ] "x" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[] tree >>= fun c0 ->
      S.Tree.add tree [ "c"; "b"; "a1" ] "x1" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c0 ] tree >>= fun c1 ->
      S.Tree.add tree [ "c"; "b"; "a2" ] "x2" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c1 ] tree >>= fun c2 ->
      S.Tree.add tree [ "c"; "b"; "a3" ] "x3" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c1 ] tree >>= fun c3 ->
      Lwt.return (c2, c3)
    in
    (*
     \->  c5 -> c6 -> c7 |freeze|
     \->  c4      \-> c8
    *)
    let tree2 repo =
      let tree = S.Tree.empty in
      S.Tree.add tree [ "c"; "b1" ] "x4" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[] tree >>= fun c4 ->
      S.Tree.add tree [ "c"; "b2" ] "x5" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[] tree >>= fun c5 ->
      S.Tree.add tree [ "c"; "b1"; "a" ] "x6" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c5 ] tree >>= fun c6 ->
      S.Tree.add tree [ "c"; "e" ] "x7" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c6 ] tree >>= fun c7 ->
      S.Tree.add tree [ "c"; "d" ] "x8" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[ S.Commit.hash c6 ] tree >>= fun c8 ->
      Lwt.return (c4, c5, c7, c8)
    in
    let test repo =
      tree1 repo >>= fun (c2, c3) ->
      P.Commit.find (h repo) (S.Commit.hash c2) >>= fun t2 ->
      S.freeze repo ~max:[ c2 ] >>= fun () ->
      P.Commit.find (h repo) (S.Commit.hash c2) >>= fun t2' ->
      check_val "c2" t2 t2';
      P.Commit.find (h repo) (S.Commit.hash c3) >>= fun t3 ->
      check_val "c3" None t3;
      let tree = S.Commit.tree c2 in
      S.Tree.find tree [ "c"; "b"; "a1" ] >>= fun x1 ->
      Alcotest.(check (option string)) "x1" (Some "x1") x1;
      S.Tree.find tree [ "c"; "b"; "a3" ] >>= fun x3 ->
      Alcotest.(check (option string)) "x3" None x3;
      S.of_commit c2 >>= fun a ->
      S.tree a >>= fun tree_a ->
      S.Tree.find tree_a [ "c"; "b"; "a2" ] >>= fun x2 ->
      Alcotest.(check (option string)) "x2" (Some "x2") x2;
      S.Tree.find tree_a [ "c"; "b"; "a3" ] >>= fun x3 ->
      Alcotest.(check (option string)) "x3" None x3;
      tree2 repo >>= fun (c4, c5, c7, c8) ->
      P.Commit.find (h repo) (S.Commit.hash c5) >>= fun t5 ->
      P.Commit.find (h repo) (S.Commit.hash c7) >>= fun t7 ->
      S.freeze repo ~max:[ c7 ] >>= fun () ->
      P.Commit.find (h repo) (S.Commit.hash c5) >>= fun t5' ->
      check_val "c5" t5 t5';
      P.Commit.find (h repo) (S.Commit.hash c7) >>= fun t7' ->
      check_val "c7" t7 t7';
      P.Commit.find (h repo) (S.Commit.hash c4) >>= fun t4 ->
      check_val "c4" None t4;
      P.Commit.find (h repo) (S.Commit.hash c8) >>= fun t8 ->
      check_val "c8" None t8;
      (S.Commit.of_hash repo (S.Commit.hash c8) >>= function
       | None -> Lwt.return_unit
       | Some _ -> Alcotest.fail "should not find c6")
      >>= fun () ->
      fail_with_none (S.Commit.of_hash repo (S.Commit.hash c7)) "of_hash commit"
      >>= fun c7 ->
      let tree = S.Commit.tree c7 in
      S.Tree.find tree [ "c"; "e" ] >>= fun x7 ->
      Alcotest.(check (option string)) "x7" (Some "x7") x7;
      S.Repo.close repo
    in
    run x test

  (* branches that point to deleted commits are deleted as well *)
  let test_fail_branch x () =
    let check_val repo = check (T.option @@ S.commit_t repo) in
    let test repo =
      r1 ~repo >>= fun kv1 ->
      r2 ~repo >>= fun kv2 ->
      S.Branch.set repo b1 kv1 >>= fun () ->
      S.Branch.set repo b2 kv2 >>= fun () ->
      S.freeze repo ~max:[ kv1 ] >>= fun () ->
      S.Branch.find repo b1 >>= fun k1' ->
      check_val repo "r1 after freeze" (Some kv1) k1';
      S.Branch.find repo b2 >>= fun k2' ->
      check_val repo "r2 deleted by freeze" None k2';
      S.Repo.close repo
    in
    run x test

  let test_set x () =
    let check_list = checks T.(pair S.Key.step_t S.kind_t) in
    let check_parents = checks S.Hash.t in
    let test repo =
      S.master repo >>= fun t ->
      S.set_exn t [ "a"; "b"; "c" ] v1 ~info:(infof "commit 1") >>= fun () ->
      S.Head.get t >>= fun c1 ->
      S.freeze repo ~max:[ c1 ] >>= fun () ->
      S.layer_id repo (S.Commit_t (S.Commit.hash c1)) >>= fun s ->
      Alcotest.(check string) "layer id of commit 1" s "lower";
      S.set_exn t [ "a"; "d" ] v2 ~info:(infof "commit 2") >>= fun () ->
      S.list t [ "a" ] >>= fun ks ->
      (* list sees a merged tree from lower and upper layers *)
      check_list "path" [ ("d", `Contents); ("b", `Node) ] ks;
      S.Head.get t >>= fun c2 ->
      S.layer_id repo (S.Commit_t (S.Commit.hash c2)) >>= fun s ->
      Alcotest.(check string) "layer_id commit 2" s "upper1";
      let parents = S.Commit.parents c2 in
      check_parents "parents of c2" [ S.Commit.hash c1 ] parents;
      S.get t [ "a"; "b"; "c" ] >>= fun s ->
      (* read value from lower layers *)
      Alcotest.(check string) "commit 1" s v1;
      S.set_exn t [ "a"; "b"; "c" ] "Hello" ~info:(infof "commit 3")
      >>= fun () ->
      S.get t [ "a"; "b"; "c" ] >>= fun s ->
      (* updated value on upper layers, hides lower layer *)
      Alcotest.(check string) "commit 3" s "Hello";
      S.Repo.close repo
    in
    run x test

  (* test set tree, add and remove to a tree; test get head from lower*)
  let test_set_tree x () =
    let fail_with_some f msg =
      f >>= function None -> Lwt.return_unit | Some _ -> Alcotest.fail msg
    in
    let test repo =
      S.master repo >>= fun t ->
      S.Tree.add S.Tree.empty [ "a"; "d" ] v1 >>= fun t1 ->
      S.Tree.add t1 [ "a"; "b"; "c" ] v2 >>= fun t1 ->
      S.set_tree_exn ~info:(infof "commit 1") ~parents:[] t [] t1 >>= fun () ->
      S.freeze repo >>= fun () ->
      (* get head from lower *)
      S.Head.get t >>= fun c1 ->
      let t1 = S.Commit.tree c1 in
      fail_with_none (S.Tree.find t1 [ "a"; "d" ]) "find in t1" >>= fun v ->
      Alcotest.(check string) "t1" v v1;
      fail_with_none (S.Tree.find t1 [ "a"; "b"; "c" ]) "find in t1"
      >>= fun v ->
      Alcotest.(check string) "t1" v v2;
      S.Tree.remove t1 [ "a"; "b"; "c" ] >>= fun t2 ->
      S.set_tree_exn ~info:(infof "commit 2") ~parents:[ c1 ] t [] t2
      >>= fun () ->
      S.get_tree t [] >>= fun t3 ->
      fail_with_some (S.Tree.find t3 [ "a"; "b"; "c" ]) "found after remove"
      >>= fun () ->
      fail_with_none (S.Tree.find t3 [ "a"; "d" ]) "find in t3" >>= fun v ->
      Alcotest.(check string) "t3" v v1;
      S.Repo.branches repo >>= Lwt_list.iter_p (S.Branch.remove repo)
      >>= fun () -> S.Repo.close repo
    in
    run x test

  (* use branch deleted by freeze in merges *)
  let test_merge_unrelated x () =
    let test1 repo =
      S.of_branch repo "foo" >>= fun foo ->
      S.of_branch repo "bar" >>= fun bar ->
      S.set_exn foo ~info:(infof "update foo:a") [ "a" ] v1 >>= fun () ->
      S.set_exn bar ~info:(infof "update bar:b") [ "b" ] v1 >>= fun () ->
      S.Head.get foo >>= fun c ->
      S.freeze repo ~max:[ c ] >>= fun () ->
      Lwt.catch
        (fun () ->
          S.merge_into ~info:(infof "merge bar into foo") bar ~into:foo
          >|= fun _ -> Alcotest.fail "bar should point to a deleted commit")
        (function
          | Invalid_argument msg ->
              if msg = "merge_with_branch: bar is not a valid branch ID" then
                P.Repo.close repo
              else Lwt.fail (Failure msg)
          | exn -> Lwt.fail exn)
    in
    let test2 repo =
      S.of_branch repo "foo" >>= fun foo ->
      S.of_branch repo "bar" >>= fun bar ->
      S.set_exn foo ~info:(infof "update foo:a") [ "a" ] v1 >>= fun () ->
      S.set_exn bar ~info:(infof "update bar:b") [ "b" ] v1 >>= fun () ->
      S.Head.get bar >>= fun c ->
      S.freeze repo ~max:[ c ] >>= fun () ->
      S.merge_into ~info:(infof "merge bar into foo") bar ~into:foo
      >>= merge_exn "merge unrelated"
      >>= fun () ->
      S.get bar [ "b" ] >>= fun v1' ->
      check S.contents_t "v1" v1 v1';
      Lwt.catch
        (fun () ->
          S.get foo [ "a" ] >|= fun _ ->
          Alcotest.fail "foo should point to a deleted commit")
        (function
          | Invalid_argument msg ->
              if msg = "Irmin.Tree.get: /a not found" then P.Repo.close repo
              else Lwt.fail (Failure msg)
          | exn -> Lwt.fail exn)
    in
    run x test1;
    run x test2

  (* TODO check size of the store before and after freeze *)
  let test_squash x () =
    let check_val = check T.(option S.contents_t) in
    let test repo =
      S.master repo >>= fun t ->
      S.Tree.reset_counters ();
      S.set_exn t ~info:(infof "add x/y/z") [ "x"; "y"; "z" ] v1 >>= fun () ->
      S.Head.get t >>= fun c ->
      S.get_tree t [ "x" ] >>= fun tree ->
      S.set_tree_exn t ~info:(infof "update") [ "u" ] tree >>= fun () ->
      S.Head.get t >>= fun c1 ->
      S.set_exn t ~info:(infof "add u/x/y") [ "u"; "x"; "y" ] v2 >>= fun () ->
      S.Head.get t >>= fun c2 ->
      S.Tree.add tree [ "x"; "z" ] v1 >>= fun tree3 ->
      S.set_tree_exn t ~info:(infof "update") [ "u" ] tree3 >>= fun () ->
      S.Head.get t >>= fun c3 ->
      S.tree t >>= fun tree3 ->
      S.Tree.stats ~force:true tree3 >>= fun s ->
      Log.debug (fun l ->
          l
            "stats before freeze nodes = %d, leafs = %d, depth = %d, width = \
             %d, skips = %d"
            s.nodes s.leafs s.depth s.width s.skips);
      Log.debug (fun l -> l "counters = %a" S.Tree.dump_counters ());
      S.freeze repo ~squash:true >>= fun () ->
      S.tree t >>= fun tree ->
      S.set_tree_exn t ~info:(infof "update") [] tree >>= fun () ->
      S.Tree.stats ~force:true tree >>= fun s ->
      Log.debug (fun l ->
          l
            "stats after freeze nodes = %d, leafs = %d, depth = %d, width = \
             %d, skips = %d"
            s.nodes s.leafs s.depth s.width s.skips);
      Log.debug (fun l -> l "counters = %a" S.Tree.dump_counters ());
      (S.Commit.of_hash repo (S.Commit.hash c) >>= function
       | None -> Lwt.return_unit
       | Some _ -> Alcotest.fail "should not find c")
      >>= fun () ->
      (S.Commit.of_hash repo (S.Commit.hash c1) >>= function
       | None -> Lwt.return_unit
       | Some _ -> Alcotest.fail "should not find c1")
      >>= fun () ->
      (S.Commit.of_hash repo (S.Commit.hash c2) >>= function
       | None -> Lwt.return_unit
       | Some _ -> Alcotest.fail "should not find c2")
      >>= fun () ->
      (S.Commit.of_hash repo (S.Commit.hash c3) >>= function
       | None -> Alcotest.fail "should not find c3"
       | Some _ -> Lwt.return_unit)
      >>= fun () ->
      S.find t [ "x"; "y"; "z" ] >>= fun v1' ->
      check_val "x/y/z after merge" (Some v1) v1';
      S.find t [ "u"; "x"; "z" ] >>= fun v1' ->
      check_val "u/x/z after merge" (Some v1) v1';
      S.find t [ "u"; "y"; "z" ] >>= fun v1' ->
      check_val "u/y/z after merge" (Some v1) v1';
      S.find t [ "u"; "x"; "y" ] >>= fun v2' ->
      check_val "vy after merge" None v2';
      P.Repo.close repo
    in
    run x test

  let test_branch_squash x () =
    let check_val = check T.(option S.contents_t) in
    let setup repo =
      S.Tree.add S.Tree.empty [ "a"; "b"; "c" ] v1 >>= fun tree1 ->
      S.Tree.add S.Tree.empty [ "a"; "b"; "d" ] v2 >>= fun tree2 ->
      S.of_branch repo "foo" >>= fun foo ->
      S.set_tree_exn ~parents:[] ~info:(infof "tree1") foo [] tree1
      >>= fun () ->
      S.Head.get foo >>= fun c1 ->
      S.master repo >>= fun t ->
      S.set_tree_exn ~parents:[] ~info:(infof "tree2") t [] tree2 >>= fun () ->
      S.Head.get t >|= fun c2 -> (c1, c2)
    in
    let test repo =
      setup repo >>= fun (c1, c2) ->
      S.freeze repo ~squash:true >>= fun () ->
      (P.Commit.mem (P.Repo.commit_t repo) (S.Commit.hash c1) >>= function
       | true ->
           S.layer_id repo (S.Commit_t (S.Commit.hash c1)) >|= fun s ->
           Alcotest.(check string) "layer_id commit 1" s "lower"
       | false ->
           Alcotest.failf "did not copy commit %a to dst" S.Commit.pp_hash c1)
      >>= fun () ->
      (P.Commit.mem (P.Repo.commit_t repo) (S.Commit.hash c2) >>= function
       | true ->
           S.layer_id repo (S.Commit_t (S.Commit.hash c2)) >|= fun s ->
           Alcotest.(check string) "layer_id commit 2" s "lower"
       | false ->
           Alcotest.failf "did not copy commit %a to dst" S.Commit.pp_hash c2)
      >>= fun () ->
      S.of_branch repo "foo" >>= fun foo ->
      S.find foo [ "a"; "b"; "c" ] >>= fun v1' ->
      check_val "copy v1 to dst" (Some v1) v1';
      S.master repo >>= fun t ->
      S.find t [ "a"; "b"; "d" ] >>= fun v2' ->
      check_val "copy v2 to dst" (Some v2) v2';
      P.Repo.close repo
    in
    let test_squash repo =
      setup repo >>= fun (c1, c2) ->
      S.freeze repo ~squash:true ~max:[ c2 ] >>= fun () ->
      (P.Commit.mem (P.Repo.commit_t repo) (S.Commit.hash c1) >|= function
       | true ->
           Alcotest.failf "should not copy commit %a to dst" S.Commit.pp_hash c1
       | false -> ())
      >>= fun () ->
      (P.Commit.mem (P.Repo.commit_t repo) (S.Commit.hash c2) >>= function
       | true ->
           S.layer_id repo (S.Commit_t (S.Commit.hash c2)) >|= fun s ->
           Alcotest.(check string) "layer_id commit 2" s "lower"
       | false ->
           Alcotest.failf "should copy commit %a to dst" S.Commit.pp_hash c2)
      >>= fun () ->
      S.master repo >>= fun t ->
      S.find t [ "a"; "b"; "c" ] >>= fun v1' ->
      check_val "copy v1 to dst" None v1';
      S.find t [ "a"; "b"; "d" ] >>= fun v2' ->
      check_val "copy v2 to dst" (Some v2) v2';
      (S.Branch.find repo "foo" >|= function
       | None -> ()
       | Some _ -> Alcotest.failf "should not find branch foo in dst")
      >>= fun () -> P.Repo.close repo
    in
    run x test;
    run x test_squash

  let test_consecutive_freeze x () =
    let test repo =
      let info = info "freezes" in
      let check_val repo = check (T.option @@ S.commit_t repo) in
      let tree = S.Tree.empty in
      let rec commits tree acc i =
        if i = 10 then Lwt.return acc
        else
          let a = "a" ^ string_of_int i in
          let y = "y" ^ string_of_int i in
          let b = "b" ^ string_of_int i in
          S.Tree.add tree [ "c"; "b"; a ] y >>= fun tree ->
          Log.debug (fun l -> l "commit %s" y);
          S.Commit.v repo ~info ~parents:[] tree >>= fun c ->
          S.Branch.set repo b c >>= fun () ->
          S.freeze repo ~max:[ c ] >>= fun () -> commits tree (c :: acc) (i + 1)
      in
      let tests c i =
        let tree = S.Commit.tree c in
        let a = "a" ^ string_of_int i in
        let y = "y" ^ string_of_int i in
        let b = "b" ^ string_of_int i in
        Log.debug (fun l -> l "test %s" y);
        S.Tree.find tree [ "c"; "b"; a ] >>= fun y' ->
        Alcotest.(check (option string)) "commit" (Some y) y';
        S.layer_id repo (S.Commit_t (S.Commit.hash c)) >>= fun s ->
        Alcotest.(check string) "layer id of commit" s "lower";
        S.Branch.find repo b >|= fun c' -> check_val repo "branch" (Some c) c'
      in
      commits tree [] 0 >>= fun commits ->
      Lwt_list.iteri_s (fun i c -> tests c i) (List.rev commits) >>= fun () ->
      S.Repo.close repo
    in
    run x test

  let test_freeze_tree x () =
    let info = info "two" in
    let test repo =
      let find4 tree =
        S.Tree.find tree [ "4" ] >|= fun x ->
        Alcotest.(check (option string)) "x4" (Some "x4") x
      in
      let find5 tree () =
        Log.debug (fun l -> l "find2");
        S.Tree.find tree [ "5" ] >|= fun x ->
        Alcotest.(check (option string)) "x5" (Some "x5") x
      in
      let tree = S.Tree.empty in
      S.Tree.add tree [ "1"; "2"; "3" ] "x1" >>= fun tree ->
      S.Tree.add tree [ "4" ] "x4" >>= fun tree ->
      S.Tree.add tree [ "5" ] "x5" >>= fun tree ->
      S.Commit.v repo ~info ~parents:[] tree >>= fun h ->
      S.Tree.clear tree;
      S.Commit.of_hash repo (S.Commit.hash h) >>= function
      | None -> Alcotest.fail "commit not found"
      | Some commit ->
          let tree = S.Commit.tree commit in
          find4 tree >>= fun () ->
          S.freeze ~max:[ h ] repo >>= fun () ->
          (* S.freeze ~keep_max:true ~max:[ h ] repo >>= fun () ->
             S.PrivateLayer.wait_for_freeze () >>= fun () ->
          *)
          find5 tree () >>= fun () -> S.Repo.close repo
    in
    run x test
end
