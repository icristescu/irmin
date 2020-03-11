open Crowbar
module T = Irmin.Type

let ( |+ ) = T.( |+ )

let ( |~ ) = T.( |~ )

let fmt = Format.sprintf

(* This is part of the standard library as of 4.08 (see [Option.map]). *)
let option_map f o = match o with None -> None | Some t -> Some (f t)

(* This is part of the standard library as of 4.08 (see [Result.map] and [Result.map_error]). *)
let result_map fa fe r =
  match r with Ok a -> Ok (fa a) | Error e -> Error (fe e)

(* This has to be upstreamed to Crowbar. In the meantime, you can run:
   {v opam pin crowbar https://github.com/stedolan/crowbar.git#master v}. *)
let char = map [ bytes_fixed 1 ] (fun s -> s.[0])

let string = bytes

let bytes = map [ string ] Bytes.of_string

let triple a b c = map [ a; b; c ] (fun a b c -> (a, b, c))

(* Untyped internal representation of Irmin values. *)
type any_value =
  | VUnit of unit
  | VBool of bool
  | VChar of char
  | VInt of int
  | VInt32 of int32
  | VInt64 of int64
  | VFloat of float
  | VString of string
  | VBytes of bytes
  | VList of any_value list
  | VArray of any_value array
  | VOption of any_value option
  | VPair of any_value * any_value
  | VTriple of any_value * any_value * any_value
  | VResult of (any_value, any_value) result
  | VRecord of dyn_record
  | VVariant of dyn_variant
  | VEnum of dyn_variant

(* Since we can't use "real" records and variants to test how Irmin serializes
   and deserializes them, we instead represent records with a hashtable of all
   their fields, and variants as the name of the current constructor and its value. *)
and dyn_record = (string * (string, any_value) Hashtbl.t[@opaque])

and dyn_variant = string * string * any_value

(** Pretty-print an [any_value] value. This should be generated by ppx_deriving,
    but we can't easily use it in combination with the custom rewriter. *)
let rec show_any_value = function
  | VUnit _ -> "VUnit ()"
  | VBool b -> fmt "VBool %B" b
  | VChar c -> fmt "VChar %C" c
  | VInt i -> fmt "VInt %d" i
  | VInt32 i -> fmt "VInt32 %ldl" i
  | VInt64 i -> fmt "VInt64 %LdL" i
  | VFloat f -> fmt "VFloat %F" f
  | VString s -> fmt "VString %S" s
  | VBytes b -> fmt "VBytes %S" (Bytes.to_string b)
  | VList l ->
      l |> List.map show_any_value |> String.concat "; " |> fmt "VList [%s]"
  | VArray a ->
      a
      |> Array.to_list
      |> List.map show_any_value
      |> String.concat "; "
      |> fmt "VArray [|%s|]"
  | VOption (Some v) -> fmt "VOption (Some (%s))" (show_any_value v)
  | VOption None -> "VOption None"
  | VPair (a, b) -> fmt "VPair (%s, %s)" (show_any_value a) (show_any_value b)
  | VTriple (a, b, c) ->
      fmt "VTriple (%s, %s, %s)" (show_any_value a) (show_any_value b)
        (show_any_value c)
  | VResult (Ok v) -> fmt "VResult (Ok (%s))" (show_any_value v)
  | VResult (Error v) -> fmt "VResult (Error (%s))" (show_any_value v)
  | VRecord r -> fmt "VRecord (%s)" (show_dyn_record r)
  | VVariant v -> fmt "VVariant (%s)" (show_dyn_variant v)
  | VEnum v -> fmt "VEnum (%s)" (show_dyn_variant v)

and show_dyn_record (n, t) =
  fmt "{ name: %S; %s }" n
    (String.concat "; "
       (Hashtbl.fold
          (fun k x xs -> fmt "%S: (%s)" k (show_any_value x) :: xs)
          t []))

and show_dyn_variant (n, c, v) =
  fmt "{ name: %S; current_case: %S; value: %S }" n c (show_any_value v)

(** Internal representation of Irmin types. *)
type 'a t =
  | TUnit : unit t
  | TBool : bool t
  | TChar : char t
  | TInt : int t
  | TInt32 : int32 t
  | TInt64 : int64 t
  | TFloat : float t
  | TString : string t
  | TBytes : bytes t
  | TList : 'a t -> 'a list t
  | TArray : 'a t -> 'a array t
  | TOption : 'a t -> 'a option t
  | TPair : 'a t * 'b t -> ('a * 'b) t
  | TTriple : 'a t * 'b t * 'c t -> ('a * 'b * 'c) t
  | TResult : 'a t * 'e t -> ('a, 'e) result t
  (* Each of these lists must have 1 to 5 elements. *)
  | TRecord : string * field list -> dyn_record t
  | TVariant : string * case list -> dyn_variant t
  | TEnum : string * string list -> dyn_variant t

and field = string * any_type

and case = string * any_case_type

and 'a case_type =
  | Case0 : dyn_variant case_type
  | Case1 : 'a t -> ('a -> dyn_variant) case_type

(* Allows heterogeneous types to be handled together. *)
and any_type = AT : 'a t -> any_type

and any_case_type = ACT : 'a case_type -> any_case_type

(** Pretty-print an [any_type] value. This should be generated by ppx_deriving,
    but it doesn't support GADTs (see issue 7 of ocaml-ppx/ppx_deriving). *)
let rec show_any_type = function
  | AT TUnit -> "TUnit"
  | AT TBool -> "TBool"
  | AT TChar -> "TChar"
  | AT TInt -> "TInt"
  | AT TInt32 -> "TInt32"
  | AT TInt64 -> "TInt64"
  | AT TFloat -> "TFloat"
  | AT TString -> "TString"
  | AT TBytes -> "TBytes"
  | AT (TList t) -> fmt "TList (%s)" (show_any_type (AT t))
  | AT (TArray t) -> fmt "TArray (%s)" (show_any_type (AT t))
  | AT (TOption t) -> fmt "TOption (%s)" (show_any_type (AT t))
  | AT (TPair (ta, tb)) ->
      fmt "TPair (%s, %s)" (show_any_type (AT ta)) (show_any_type (AT tb))
  | AT (TTriple (ta, tb, tc)) ->
      fmt "TTriple (%s, %s, %s)" (show_any_type (AT ta)) (show_any_type (AT tb))
        (show_any_type (AT tc))
  | AT (TResult (ta, te)) ->
      fmt "TResult (%s, %s)" (show_any_type (AT ta)) (show_any_type (AT te))
  | AT (TRecord (name, fields)) ->
      let show_field (n, t) = fmt "(%s, %s)" n (show_any_type t) in
      let fields_fmt = String.concat "; " (List.map show_field fields) in
      fmt "TRecord (%s, [%s])" name fields_fmt
  | AT (TVariant (name, cases)) ->
      let show_any_case_type = function
        | ACT Case0 -> "Case0"
        | ACT (Case1 t) -> fmt "Case1 %s" (show_any_type (AT t))
      in
      let show_case (n, ct) = fmt "(%s, %s)" n (show_any_case_type ct) in
      let cases_fmt = String.concat "; " (List.map show_case cases) in
      fmt "TVariant (%s, [%s])" name cases_fmt
  | AT (TEnum (name, cases)) ->
      let cases_fmt = String.concat "; " cases in
      fmt "TEnum (%s, [%s])" name cases_fmt

(** Wrap a value of the given dynamic type into an [any_value]. *)
let rec wrap : type a. a t -> a -> any_value =
 fun t v ->
  match t with
  | TUnit -> VUnit v
  | TBool -> VBool v
  | TChar -> VChar v
  | TInt -> VInt v
  | TInt32 -> VInt32 v
  | TInt64 -> VInt64 v
  | TFloat -> VFloat v
  | TString -> VString v
  | TBytes -> VBytes v
  | TList t' -> VList (List.map (wrap t') v)
  | TArray t' -> VArray (Array.map (wrap t') v)
  | TOption t' -> VOption (option_map (wrap t') v)
  | TPair (t1, t2) ->
      let v1, v2 = v in
      VPair (wrap t1 v1, wrap t2 v2)
  | TTriple (t1, t2, t3) ->
      let v1, v2, v3 = v in
      VTriple (wrap t1 v1, wrap t2 v2, wrap t3 v3)
  | TResult (ta, te) -> VResult (result_map (wrap ta) (wrap te) v)
  | TRecord _ -> VRecord v
  | TVariant _ -> VVariant v
  | TEnum _ -> VEnum v

(** Unwrap an [any_value] of the given dynamic type. *)
let rec unwrap : type a. a t -> any_value -> a =
 fun t w ->
  match (t, w) with
  | TUnit, VUnit v -> v
  | TBool, VBool v -> v
  | TChar, VChar v -> v
  | TInt, VInt v -> v
  | TInt32, VInt32 v -> v
  | TInt64, VInt64 v -> v
  | TFloat, VFloat v -> v
  | TString, VString v -> v
  | TBytes, VBytes v -> v
  | TList t', VList v -> List.map (unwrap t') v
  | TArray t', VArray v -> Array.map (unwrap t') v
  | TOption t', VOption v -> option_map (unwrap t') v
  | TPair (t1, t2), VPair (v1, v2) -> (unwrap t1 v1, unwrap t2 v2)
  | TTriple (t1, t2, t3), VTriple (v1, v2, v3) ->
      (unwrap t1 v1, unwrap t2 v2, unwrap t3 v3)
  | TResult (ta, te), VResult v -> result_map (unwrap ta) (unwrap te) v
  | TRecord _, VRecord v -> v
  | TVariant _, VVariant v -> v
  | TEnum _, VEnum v -> v
  | _ ->
      failwith
      @@ fmt "Tried to unwrap %s while expecting type %s." (show_any_value w)
           (show_any_type (AT t))

(** Check whether [l] has unique elements using comparison function [cmp]. *)
let is_unique_list (type a) ?(cmp : a -> a -> int = compare) =
  let module S = Set.Make (struct
    type t = a

    let compare = cmp
  end) in
  let rec aux set = function
    | [] -> true
    | x :: xs when S.find_opt x set = None -> aux (S.add x set) xs
    | _ -> false
  in
  aux S.empty

(** Ensure that the associative list [l] has 5 or less entries with unique keys. *)
let guard_keys l =
  guard
    ( List.length l <= 5
    && is_unique_list ~cmp:(fun (s1, _) (s2, _) -> String.compare s1 s2) l );
  l

(** Ensure that the list [l] has 5 or less unique values. *)
let guard_strings l =
  guard (List.length l <= 5 && is_unique_list ~cmp:String.compare l);
  l

(** Generate a dynamic type recursively. *)
let any_type_gen =
  fix (fun at_gen ->
      let field_gen = pair string at_gen in
      let case_type_gen =
        choose
          [ const (ACT Case0); map [ at_gen ] (fun (AT at) -> ACT (Case1 at)) ]
      in
      let case_gen = pair string case_type_gen in
      let fields_gen = map [ list1 field_gen ] guard_keys in
      let cases_gen = map [ list1 case_gen ] guard_keys in
      let tags_gen = map [ list1 string ] guard_strings in
      choose
        [
          const (AT TUnit);
          const (AT TBool);
          const (AT TChar);
          (* const (AT TInt);
             const (AT TInt32);
             const (AT TInt64);
             const (AT TFloat); *)
          const (AT TString);
          const (AT TBytes);
          map [ at_gen ] (fun (AT t) -> AT (TList t));
          map [ at_gen ] (fun (AT t) -> AT (TArray t));
          map [ at_gen ] (fun (AT t) -> AT (TOption t));
          map [ at_gen; at_gen ] (fun (AT t1) (AT t2) -> AT (TPair (t1, t2)));
          map [ at_gen; at_gen; at_gen ] (fun (AT t1) (AT t2) (AT t3) ->
              AT (TTriple (t1, t2, t3)));
          map [ at_gen; at_gen ] (fun (AT t1) (AT t2) -> AT (TResult (t1, t2)));
          map [ string; fields_gen ] (fun s fs -> AT (TRecord (s, fs)));
          map [ string; cases_gen ] (fun s cs -> AT (TVariant (s, cs)));
          map [ string; tags_gen ] (fun s ts -> AT (TEnum (s, ts)));
        ])

(** Convert a [t] into its [Irmin.Type] counterpart. *)
let rec t_to_irmin : type a. a t -> a T.ty = function
  | TUnit -> T.unit
  | TBool -> T.bool
  | TChar -> T.char
  | TInt -> T.int
  | TInt32 -> T.int32
  | TInt64 -> T.int64
  | TFloat -> T.float
  | TString -> T.string
  | TBytes -> T.bytes
  | TList t -> T.list (t_to_irmin t)
  | TArray t -> T.array (t_to_irmin t)
  | TOption t -> T.option (t_to_irmin t)
  | TPair (t1, t2) -> T.pair (t_to_irmin t1) (t_to_irmin t2)
  | TTriple (t1, t2, t3) ->
      T.triple (t_to_irmin t1) (t_to_irmin t2) (t_to_irmin t3)
  | TResult (t1, t2) -> T.result (t_to_irmin t1) (t_to_irmin t2)
  | TRecord (n, fs) -> irmin_record n fs
  | TVariant (n, cs) -> irmin_variant n cs
  | TEnum (n, cs) -> irmin_enum n cs

(** Dynamically build an Irmin record (assuming it has 1 to 5 fields). *)
and irmin_record : string -> field list -> dyn_record T.t = [%impl_record 5]

(** Create a [dyn_record] from a list of [string * any_value] pairs. *)
and new_dyn_record : string -> (string * any_value) list -> dyn_record =
 fun name fields ->
  let record = Hashtbl.create (List.length fields) in
  List.iter (fun (k, v) -> Hashtbl.add record k v) fields;
  (name, record)

(** Create a function which retrieves a value of the given dynamic type from a
    [dyn_record] or fails. *)
and new_dyn_record_getter : type a. string -> string -> a t -> dyn_record -> a =
 fun record_name field_name field_type dynrec ->
  let curr_record_name, curr_table = dynrec in
  if not (String.equal record_name curr_record_name) then
    failwith "Trying to access fields from the wrong record."
  else
    let wrapped = Hashtbl.find curr_table field_name in
    unwrap field_type wrapped

(** Dynamically build an Irmin variant (assuming it has 1 to 5 cases). *)
and irmin_variant : string -> case list -> dyn_variant T.t = [%impl_variant 5]

(** Dynamically build an Irmin enum (assuming it has 1 to 5 cases). *)
and irmin_enum : string -> string list -> dyn_variant T.t =
 fun n ts -> irmin_variant n (List.map (fun t -> (t, ACT Case0)) ts)

(** Create a generator for values of a given dynamic type. *)
let rec t_to_value_gen : type a. a t -> a gen = function
  | TUnit -> const ()
  | TBool -> bool
  | TChar -> char
  | TInt -> int
  | TInt32 -> int32
  | TInt64 -> int64
  | TFloat -> float
  | TString -> string
  | TBytes -> bytes
  | TList ty -> list (t_to_value_gen ty)
  | TArray ty -> map [ list (t_to_value_gen ty) ] Array.of_list
  | TOption ty -> option (t_to_value_gen ty)
  | TPair (ty1, ty2) -> pair (t_to_value_gen ty1) (t_to_value_gen ty2)
  | TTriple (ty1, ty2, ty3) ->
      triple (t_to_value_gen ty1) (t_to_value_gen ty2) (t_to_value_gen ty3)
  | TResult (ty1, ty2) -> result (t_to_value_gen ty1) (t_to_value_gen ty2)
  | TRecord (n, fs) -> irmin_record_gen n fs
  | TVariant (n, cs) -> irmin_variant_gen n cs
  | TEnum (n, ts) -> irmin_enum_gen n ts

(** Create a generator for an Irmin record with the given specification. *)
and irmin_record_gen : string -> field list -> dyn_record gen =
 fun record_name fields ->
  let rec fields_gen = function
    | [] -> const []
    | (field_name, AT field_type) :: xs ->
        map [ t_to_value_gen field_type; fields_gen xs ] (fun v fs' ->
            (field_name, wrap field_type v) :: fs')
  in
  map [ fields_gen fields ] (fun fs' -> new_dyn_record record_name fs')

(** Create a generator for an Irmin variant with the given specification. *)
and irmin_variant_gen : string -> case list -> dyn_variant gen =
 fun variant_name cases ->
  let rec cases_gen = function
    | [] -> []
    | (case_name, ACT Case0) :: xs ->
        const (variant_name, case_name, VUnit ()) :: cases_gen xs
    | (case_name, ACT (Case1 t)) :: xs ->
        map [ t_to_value_gen t ] (fun v -> (variant_name, case_name, wrap t v))
        :: cases_gen xs
  in
  choose (cases_gen cases)

(** Create a generator for an Irmin enum with the given specification. *)
and irmin_enum_gen : string -> string list -> dyn_variant gen =
 fun n ts -> irmin_variant_gen n (List.map (fun t -> (t, ACT Case0)) ts)

type inhabited = Inhabited : 'a t * 'a -> inhabited

(** Generate a [Inhabited (t, v)], where [t : 'a t] is a dynamic type and
    [v : 'a] is a corresponding value. *)
let inhabited_gen : inhabited gen =
  dynamic_bind any_type_gen @@ fun (AT t) ->
  map [ t_to_value_gen t ] (fun v -> Inhabited (t, v))

(** Pretty-print a value of a given dynamic type. [~context] can be used to
    print additional context in case of an error. *)
let pp_value :
    type a. ?context:(unit -> string) -> a t -> Format.formatter -> a -> unit =
 fun ?(context = fun () -> "") t ppf v ->
  pp ppf "Type: %s\n    Value: %s%s" (show_any_type (AT t))
    (show_any_value (wrap t v))
    (context ())

(** [bin_check (Inhabited (t, v))] checks that the value [v], of dynamic type
    [t], stays consistent after binary encoding then decoding. *)
let bin_check (Inhabited (t, v)) =
  let irmin_t = t_to_irmin t in
  let encoded = T.(unstage (to_bin_string irmin_t)) v in
  match T.(unstage (of_bin_string irmin_t)) encoded with
  | Error (`Msg e) ->
      failf "Could not deserialize binary string %s.\n\nRaised: %s." encoded e
  | Ok v' -> check_eq ~pp:(pp_value t) v v'

(** [json_check (Inhabited (t, v))] checks that the value [v], of dynamic type
    [t], stays consistent after JSON encoding then decoding. *)
let json_check (Inhabited (t, v)) =
  let irmin_t = t_to_irmin t in
  let encoded = T.to_json_string irmin_t v in
  let context () = fmt "\n    JSON: %s" encoded in
  match T.of_json_string irmin_t encoded with
  | Error (`Msg e) ->
      failf "Could not deserialize binary string %s.\n\nRaised: %s." encoded e
  | Ok v' -> check_eq ~pp:(pp_value ~context t) v v'

let () =
  add_test ~name:"T.of_bin_string and T.to_bin_string are mutually inverse."
    [ inhabited_gen ] bin_check;
  add_test ~name:"T.of_json_string and T.to_json_string are mutually inverse."
    [ inhabited_gen ] json_check
