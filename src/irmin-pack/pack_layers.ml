open Common
open Lwt.Infix

let src = Logs.Src.create "irmin.pack.layers" ~doc:"irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

module type CONFIG = Inode.CONFIG

module Default = struct
  let lower_root = "lower"

  let upper_root1 = "upper1"

  let upper_root0 = "upper0"
end

module Conf = Irmin.Private.Conf

let lower_root_key =
  Conf.key ~doc:"The root directory for the lower layer." "root_lower"
    Conf.string Default.lower_root

let lower_root conf = Conf.get conf lower_root_key

let upper_root1_key =
  Conf.key ~doc:"The root directory for the upper layer." "root_upper"
    Conf.string Default.upper_root1

let upper_root1 conf = Conf.get conf upper_root1_key

let upper_root0_key =
  Conf.key ~doc:"The root directory for the secondary upper layer."
    "root_second" Conf.string Default.upper_root0

let _upper_root0 conf = Conf.get conf upper_root0_key

let config_layers ?(conf = Conf.empty) ?(lower_root = Default.lower_root)
    ?(upper_root1 = Default.upper_root1) ?(upper_root0 = Default.upper_root0) ()
    =
  let config = Conf.add conf lower_root_key lower_root in
  let config = Conf.add config upper_root0_key upper_root0 in
  let config = Conf.add config upper_root1_key upper_root1 in
  config

module Make_ext
    (Config : CONFIG)
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S
              with type metadata = M.t
               and type hash = H.t
               and type step = P.step)
    (Commit : Irmin.Private.Commit.S with type hash = H.t) =
struct
  module Index = Pack_index.Make (H)
  module Pack = Pack.File (Index) (H)

  type store_handle =
    | Commit_t : H.t -> store_handle
    | Node_t : H.t -> store_handle
    | Content_t : H.t -> store_handle

  module X = struct
    module Hash = H

    type 'a value = { magic : char; hash : H.t; v : 'a }

    let value a =
      let open Irmin.Type in
      record "value" (fun hash magic v -> { magic; hash; v })
      |+ field "hash" H.t (fun v -> v.hash)
      |+ field "magic" char (fun v -> v.magic)
      |+ field "v" a (fun v -> v.v)
      |> sealr

    module Contents = struct
      module CA = struct
        module Key = H
        module Val = C

        module CA_Pack = Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let hash = H.hash

          let magic = 'B'

          let value = value Val.t

          let encode_bin ~dict:_ ~offset:_ v hash =
            Irmin.Type.encode_bin value { magic; hash; v }

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, t = Irmin.Type.decode_bin ~headers:false value s off in
            t.v

          let magic _ = magic
        end)

        module CA = Closeable.Pack (CA_Pack)
        include Layered.Content_addressable (H) (Index) (CA)
      end

      include Irmin.Contents.Store (CA)
    end

    module Node = struct
      module Pa = Layered.Pack_Maker (H) (Index) (Pack)
      module CA = Inode_layers.Make (Config) (H) (Pa) (Node)
      include Irmin.Private.Node.Store (Contents) (P) (M) (CA)
    end

    module Commit = struct
      module CA = struct
        module Key = H
        module Val = Commit

        module CA_Pack = Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let hash = H.hash

          let value = value Val.t

          let magic = 'C'

          let encode_bin ~dict:_ ~offset:_ v hash =
            Irmin.Type.encode_bin value { magic; hash; v }

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, v = Irmin.Type.decode_bin ~headers:false value s off in
            v.v

          let magic _ = magic
        end)

        module CA = Closeable.Pack (CA_Pack)
        include Layered.Content_addressable (H) (Index) (CA)
      end

      include Irmin.Private.Commit.Store (Node) (CA)
    end

    module Branch = struct
      module Key = B
      module Val = H
      module AW = Atomic_write (Key) (Val)
      module U = Closeable.Atomic_write (AW)
      include Layered.Atomic_write (Key) (U)
    end

    module Slice = Irmin.Private.Slice.Make (Contents) (Node) (Commit)
    module Sync = Irmin.Private.Sync.None (H) (B)

    module Repo = struct
      type t = {
        config : Irmin.Private.Conf.t;
        contents : [ `Read ] Contents.CA.t;
        node : [ `Read ] Node.CA.t;
        branch : Branch.t;
        commit : [ `Read ] Commit.CA.t;
        ucontents : [ `Read ] Contents.CA.U.t;
        unode : [ `Read ] Node.CA.U.t;
        ucommit : [ `Read ] Commit.CA.U.t;
        ubranch : Branch.U.t;
        index : Index.t;
        uindex : Index.t;
      }

      let contents_t t = t.contents

      let node_t t = (contents_t t, t.node)

      let commit_t t = (node_t t, t.commit)

      let branch_t t = t.branch

      let batch t f =
        Commit.CA.U.batch t.ucommit (fun commit ->
            Node.CA.U.batch t.unode (fun node ->
                Contents.CA.U.batch t.ucontents (fun contents ->
                    let contents = Contents.CA.project t.contents contents in
                    let node = (contents, Node.CA.project t.node node) in
                    let commit = (node, Commit.CA.project t.commit commit) in
                    f contents node commit)))

      let v_upper config =
        let root = root config in
        let root = Filename.concat root (upper_root1 config) in
        let fresh = fresh config in
        let lru_size = lru_size config in
        let readonly = readonly config in
        let log_size = index_log_size config in
        let index = Index.v ~fresh ~readonly ~log_size root in
        Contents.CA.U.v ~fresh ~readonly ~lru_size ~index root
        >>= fun ucontents ->
        Node.CA.U.v ~fresh ~readonly ~lru_size ~index root >>= fun unode ->
        Commit.CA.U.v ~fresh ~readonly ~lru_size ~index root >>= fun ucommit ->
        Branch.U.v ~fresh ~readonly root >|= fun ubranch ->
        (index, ucontents, unode, ucommit, ubranch)

      let v config =
        v_upper config >>= fun (uindex, ucontents, unode, ucommit, ubranch) ->
        let root = root config in
        let root = Filename.concat root (lower_root config) in
        let fresh = fresh config in
        let lru_size = lru_size config in
        let readonly = readonly config in
        let log_size = index_log_size config in
        let index = Index.v ~fresh ~readonly ~log_size root in
        Contents.CA.v ucontents ~fresh ~readonly ~lru_size ~index root
        >>= fun contents ->
        Node.CA.v unode ~fresh ~readonly ~lru_size ~index root >>= fun node ->
        Commit.CA.v ucommit ~fresh ~readonly ~lru_size ~index root
        >>= fun commit ->
        Branch.v ubranch ~fresh ~readonly root >|= fun branch ->
        {
          contents;
          node;
          commit;
          branch;
          config;
          index;
          ucontents;
          unode;
          ucommit;
          ubranch;
          uindex;
        }

      let close t =
        Index.close t.index;
        Index.close t.uindex;
        Contents.CA.close (contents_t t) >>= fun () ->
        Node.CA.close (snd (node_t t)) >>= fun () ->
        Commit.CA.close (snd (commit_t t)) >>= fun () -> Branch.close t.branch

      let layer_id t store_handler =
        ( match store_handler with
        | Commit_t k -> Commit.CA.layer_id t.commit k
        | Node_t k -> Node.CA.layer_id t.node k
        | Content_t k -> Contents.CA.layer_id t.contents k )
        >|= function
        | 1 -> upper_root1 t.config
        | 2 -> lower_root t.config
        | _ -> failwith "unexpected layer id"

      let clear_upper t =
        Contents.CA.U.clear t.ucontents >>= fun () ->
        Node.CA.U.clear t.unode >>= fun () ->
        Commit.CA.U.clear t.ucommit >>= fun () -> Branch.U.clear t.ubranch
    end
  end

  let null =
    match Sys.os_type with
    | "Unix" | "Cygwin" -> "/dev/null"
    | "Win32" -> "NUL"
    | _ -> invalid_arg "invalid os type"

  let integrity_check ?ppf ~auto_repair t =
    let ppf =
      match ppf with
      | Some p -> p
      | None -> open_out null |> Format.formatter_of_out_channel
    in
    Fmt.pf ppf "Running the integrity_check.\n%!";
    let nb_commits = ref 0 in
    let nb_nodes = ref 0 in
    let nb_contents = ref 0 in
    let nb_absent = ref 0 in
    let nb_corrupted = ref 0 in
    let exception Cannot_fix in
    let contents = X.Repo.contents_t t in
    let nodes = X.Repo.node_t t |> snd in
    let commits = X.Repo.commit_t t |> snd in
    let pp_stats () =
      Fmt.pf ppf "\t%dk contents / %dk nodes / %dk commits\n%!"
        (!nb_contents / 1000) (!nb_nodes / 1000) (!nb_commits / 1000)
    in
    let count_increment count =
      incr count;
      if !count mod 1000 = 0 then pp_stats ()
    in
    let f (k, (offset, length, m)) =
      match m with
      | 'B' ->
          count_increment nb_contents;
          X.Contents.CA.integrity_check ~offset ~length k contents
      | 'N' | 'I' ->
          count_increment nb_nodes;
          X.Node.CA.integrity_check ~offset ~length k nodes
      | 'C' ->
          count_increment nb_commits;
          X.Commit.CA.integrity_check ~offset ~length k commits
      | _ -> invalid_arg "unknown content type"
    in
    if auto_repair then
      try
        Index.filter t.index (fun binding ->
            match f binding with
            | Ok () -> true
            | Error `Wrong_hash -> raise Cannot_fix
            | Error `Absent_value ->
                incr nb_absent;
                false);
        if !nb_absent = 0 then Ok `No_error else Ok (`Fixed !nb_absent)
      with Cannot_fix -> Error (`Cannot_fix "Not implemented")
    else (
      Index.iter
        (fun k v ->
          match f (k, v) with
          | Ok () -> ()
          | Error `Wrong_hash -> incr nb_corrupted
          | Error `Absent_value -> incr nb_absent)
        t.index;
      if !nb_absent = 0 && !nb_corrupted = 0 then Ok `No_error
      else Error (`Corrupted (!nb_corrupted + !nb_absent)) )

  include Irmin.Of_private (X)

  module Copy = struct
    let copy_branches t =
      let commit_exists_lower = X.Commit.CA.mem_lower t.X.Repo.commit in
      X.Branch.copy t.X.Repo.branch commit_exists_lower

    let copy_contents contents t k =
      X.Contents.CA.check_and_copy t.X.Repo.contents ~dst:contents
        ~aux:(fun _ -> Lwt.return_unit)
        "Contents" k

    let copy_tree ?(skip = fun _ -> Lwt.return_false) nodes contents t root =
      X.Node.CA.mem_lower t.X.Repo.node root >>= function
      | true -> Lwt.return_unit
      | false ->
          let aux v =
            Lwt_list.iter_p
              (function
                | _, `Contents (k, _) -> copy_contents contents t k
                | _ -> Lwt.return_unit)
              (X.Node.Val.list v)
          in
          let node k =
            X.Node.CA.check_and_copy t.X.Repo.node ~dst:nodes ~aux "Node" k
          in
          Repo.iter t ~min:[] ~max:[ root ] ~node ~skip

    let copy_commit ~copy_tree commits nodes contents t k =
      let aux c = copy_tree nodes contents t (X.Commit.Val.node c) in
      X.Commit.CA.check_and_copy t.X.Repo.commit ~dst:commits ~aux "Commit" k

    module CopyToLower = struct
      let copy_tree nodes contents t root =
        let skip k = X.Node.CA.mem_lower t.X.Repo.node k in
        copy_tree ~skip nodes contents t root

      let copy_commit commits nodes contents t k =
        copy_commit ~copy_tree commits nodes contents t k

      let batch_lower t f =
        X.Commit.CA.batch_lower t.X.Repo.commit (fun commits ->
            X.Node.CA.batch_lower t.X.Repo.node (fun nodes ->
                X.Contents.CA.batch_lower t.X.Repo.contents (fun contents ->
                    f commits nodes contents)))

      let copy_max_commits t (max : commit list) =
        Log.debug (fun f -> f "copy max commits to lower");
        Lwt_list.iter_p
          (fun k ->
            let h = Commit.hash k in
            batch_lower t (fun commits nodes contents ->
                copy_commit commits nodes contents t h))
          max

      let copy_slice t slice =
        batch_lower t (fun commits nodes contents ->
            X.Slice.iter slice (function
              | `Commit (k, _) -> copy_commit commits nodes contents t k
              | _ -> Lwt.return_unit))

      let copy t ~min ~(max : commit list) () =
        Log.debug (fun f -> f "copy to lower");
        Repo.export ~full:false ~min ~max:(`Max max) t >>= copy_slice t
    end
  end

  let copy t ~squash ~min ~max () =
    Log.debug (fun f -> f "copy");
    (match max with [] -> Repo.heads t | m -> Lwt.return m) >>= fun max ->
    Lwt.catch
      (fun () ->
        (* Copy commits to lower: if squash then copy only the max commits *)
        ( if squash then Copy.CopyToLower.copy_max_commits t max
        else Copy.CopyToLower.copy t ~min ~max () )
        (* Copy branches to both lower and current_upper *)
        >>= fun () ->
        Copy.copy_branches t >|= fun () -> Ok ())
      (function
        | Layered.Copy_error e -> Lwt.return_error (`Msg e)
        | e -> Fmt.kstrf Lwt.fail_invalid_arg "copy error: %a" Fmt.exn e)

  let freeze ?(min : commit list = []) ?(max : commit list = [])
      ?(squash = false) t =
    copy t ~squash ~min ~max () >>= function
    | Ok () -> X.Repo.clear_upper t
    | Error (`Msg e) -> Fmt.kstrf Lwt.fail_with "[gc_store]: import error %s" e

  let layer_id t = X.Repo.layer_id t
end
