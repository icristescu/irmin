(*
 * Copyright (c) 2013 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Core_kernel.Std

type ('key, 'blob) t = ('key * ('key, 'blob) IrminValue.t) list
with bin_io, compare, sexp

let of_json key_of_json blob_of_json =
  Ezjsonm.(
    get_list
      (get_pair
         key_of_json
         (IrminValue.of_json key_of_json blob_of_json))
  )

let to_json json_of_key json_of_blob =
  Ezjsonm.(
    list
      (pair
         json_of_key
         (IrminValue.to_json json_of_key json_of_blob))
  )

module type S = sig
  type key
  type blob
  include Identifiable.S with type t = (key, blob) t
  val of_json: Ezjsonm.t -> t
  val to_json: t -> Ezjsonm.t
end

module S (K: IrminKey.S) (B: IrminBlob.S) = struct
  type key = K.t
  type blob = B.t
  module M = struct
    type nonrec t = (K.t, B.t) t
    with bin_io, compare, sexp
    let hash (t : t) = Hashtbl.hash t
    include Sexpable.To_stringable (struct type nonrec t = t with sexp end)
    let module_name = "Commit"
  end
  include M
  include Identifiable.Make (M)
  let of_json = of_json K.of_json B.of_json
  let to_json = to_json K.to_json B.to_json
end
